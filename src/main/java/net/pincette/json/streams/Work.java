package net.pincette.json.streams;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Projections.include;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.lang.String.join;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.shuffle;
import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.summingInt;
import static java.util.stream.Collectors.summingLong;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.Kafka.messageLag;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.streams.Common.ALIVE_AT;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.INSTANCE;
import static net.pincette.json.streams.Common.config;
import static net.pincette.json.streams.Common.removeSuffix;
import static net.pincette.mongo.JsonClient.aggregate;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.util.Collections.difference;
import static net.pincette.util.Collections.intersection;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.union;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.tryToGetSilent;
import static org.apache.kafka.clients.admin.AdminClient.create;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.logging.Level;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.json.JsonUtil;
import net.pincette.util.AsyncBuilder;
import net.pincette.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.bson.Document;

class Work {
  private static final String AVERAGE_MESSAGE_TIME_ESTIMATE = "work.averageMessageTimeEstimate";
  private static final Duration DEFAULT_AVERAGE_MESSAGE_TIME_ESTIMATE = ofMillis(50);
  private static final Duration DEFAULT_INTERVAL = ofSeconds(10);
  private static final double DEFAULT_INTERVAL_SPREAD = 0.2;
  private static final int DEFAULT_MAXIMUM_APPS_PER_INSTANCE = 10;
  private static final String INTERVAL_FIELD = "work.interval";
  private static final String INTERVAL_SPREAD = "work.intervalSpread";
  private static final String MAXIMUM_APPS_PER_INSTANCE = "work.maximumAppsPerInstance";
  private static final String MAXIMUM_MESSAGE_LAG = "maximumMessageLag";

  private final Duration averageMessageTimeEstimate;
  private final MongoCollection<Document> collection;
  private final Context context;
  private final int maximumAppsPerInstance;
  private final Random random = new Random();
  private Admin kafkaAdmin;

  Work(
      final MongoCollection<Document> collection,
      final Map<String, Object> kafkaAdminConfig,
      final Context context) {
    this.collection = collection;
    this.kafkaAdmin = create(kafkaAdminConfig);
    this.averageMessageTimeEstimate = averageMessageTimeEstimate(context);
    this.maximumAppsPerInstance = maximumAppsPerInstance(context);
    this.context = context;
  }

  private static Stream<String> applications(final List<JsonObject> list, final String pointer) {
    return list.stream()
        .map(json -> getString(json, pointer).orElse(null))
        .filter(Objects::nonNull);
  }

  private static Duration averageMessageTimeEstimate(final Context context) {
    return config(
        context,
        config -> config.getDuration(AVERAGE_MESSAGE_TIME_ESTIMATE),
        DEFAULT_AVERAGE_MESSAGE_TIME_ESTIMATE);
  }

  private static int maximumAppsPerInstance(final Context context) {
    return config(
        context,
        config -> config.getInt(MAXIMUM_APPS_PER_INSTANCE),
        DEFAULT_MAXIMUM_APPS_PER_INSTANCE);
  }

  private static <T> Set<T> pickRandom(final Set<T> set, final ToIntFunction<Set<T>> amount) {
    final List<T> all = new ArrayList<>(set);

    shuffle(all);

    return new HashSet<>(all.subList(0, amount.applyAsInt(set)));
  }

  private CompletionStage<Set<String>> allApplications() {
    return aggregate(
            collection, list(match(exists(APPLICATION_FIELD)), project(include(APPLICATION_FIELD))))
        .thenApply(list -> applications(list, "/" + APPLICATION_FIELD).collect(toSet()));
  }

  private Duration delay() {
    return ofMillis(round(interval.toMillis() * (1.0 + spread())));
  }

  private int desiredInstances(final Status status) {
    return max(
        minimumNumberOfInstances(status), status.runningInstances().size() + extraCapacity(status));
  }

  private int extraCapacity(final Status status) {
    final long capacity = instanceCapacityPerSecond();
    final long lag = status.totalExcessMessageLag();

    return (int) (lag / capacity) + (lag > 0 && lag % capacity != 0 ? 1 : 0);
  }

  CompletionStage<Boolean> giveWork() {
    return status()
        .thenApply(status -> status.map(this::giveWork).orElse(false))
        .thenComposeAsync(r -> composeAsyncAfter(this::giveWork, delay()))
        .exceptionally(
            e -> {
              severe(Stream.of(e.getMessage(), getStackTrace(e)));
              return false;
            });
  }

  private boolean giveWork(final Status status) {
    final Set<String> doingAtStart = doing.get();
    final int desired = desiredInstances(status);
    final Set<String> excess = status.runningInExcess();
    final Set<String> stopping = intersection(doingAtStart, excess);
    final Set<String> allNotRunning = status.notRunning();
    final Set<String> notRunning =
        pickPortion(
            status,
            s -> allNotRunning,
            desired,
            maximumAppsPerInstance - doingAtStart.size() + stopping.size());
    final Set<String> tooMuchMessageLag =
        pickPortion(
            status,
            s -> difference(s.tooMuchMessageLag(), union(doingAtStart, notRunning)),
            desired,
            maximumAppsPerInstance - doingAtStart.size() + stopping.size() - notRunning.size());
    final Set<String> starting = union(notRunning, tooMuchMessageLag);

    if (notRunning.isEmpty() && !allNotRunning.isEmpty()) {
      runningMoreThanOnce(status).stream()
          .findFirst()
          .ifPresent(
              r -> {
                stopping.add(r);
                starting.add(pickRandom(allNotRunning, a -> 1).stream().findFirst().orElse(null));
              });
    }

    info(
        Stream.of(
            "running: " + join(", ", doingAtStart),
            "running in excess: " + join(", ", excess),
            "stopping: " + join(", ", stopping),
            "starting: " + join(", ", starting),
            "desired instances: " + desired));

    return true;
  }

  private void info(final Stream<String> messages) {
    log(INFO, messages);
  }

  private long instanceCapacityPerSecond() {
    return 1000 / averageMessageTimeEstimate.toMillis() * maximumAppsPerInstance;
  }

  private void log(final Level level, final Stream<String> messages) {
    context.logger.log(
        level, () -> "Instance " + context.instance + ":\n  " + messages.collect(joining("\n  ")));
  }

  private CompletionStage<JsonObject> maximumMessageLag() {
    return findOne(collection, exists(MAXIMUM_MESSAGE_LAG))
        .thenApply(
            result ->
                result
                    .map(r -> r.getJsonObject(MAXIMUM_MESSAGE_LAG))
                    .orElseGet(JsonUtil::emptyObject));
  }

  private int minimumNumberOfInstances(final Status status) {
    return (status.allApplications.size() / maximumAppsPerInstance)
        + (status.allApplications.size() % maximumAppsPerInstance > 0 ? 1 : 0);
  }

  private Set<String> pickPortion(
      final Status status,
      final Function<Status, Set<String>> category,
      final int totalInstances,
      final int spareRoomForApps) {
    return pickRandom(
        category.apply(status),
        all -> min(spareRoomForApps, min(all.size(), (all.size() + 1) / totalInstances)));
  }

  private Map<String, Map<TopicPartition, Long>> removeSuffixes(
      final Map<String, Map<TopicPartition, Long>> messageLagPerGroup) {
    return messageLagPerGroup.entrySet().stream()
        .map(e -> pair(removeSuffix(e.getKey(), context), e.getValue()))
        .collect(toMap(pair -> pair.first, pair -> pair.second));
  }

  private CompletionStage<Map<String, Set<String>>> runningInstancesPerApplication() {
    return aggregate(collection, list(match(exists(ALIVE_AT)), project(include(ID))))
        .thenApply(
            list ->
                list.stream()
                    .map(json -> json.getJsonObject(ID))
                    .collect(
                        groupingBy(
                            id -> id.getString(APPLICATION_FIELD),
                            mapping(id -> id.getString(INSTANCE), toSet()))));
  }

  private Set<String> runningMoreThanOnce(final Status status) {
    return status.runningInstancesPerApplication.entrySet().stream()
        .filter(e -> e.getValue().size() > 1 && e.getValue().contains(context.instance))
        .map(Entry::getKey)
        .collect(toSet());
  }

  private void severe(final Stream<String> messages) {
    log(SEVERE, messages);
  }

  private double spread() {
    return random.nextDouble() * (random.nextBoolean() ? 1 : -1) * intervalSpread;
  }

  void start() {
    kafkaAdmin = create(kafkaAdminConfig);
    giveWork();
  }

  private CompletionStage<Optional<Status>> status() {
    return AsyncBuilder.create(Status::new)
        .update(
            status ->
                allApplications().thenApply(all -> Optional.of(status.withAllApplications(all))))
        .update(
            status ->
                runningInstancesPerApplication()
                    .thenApply(
                        running -> Optional.of(status.withRunningInstancesPerApplication(running))))
        .update(
            status ->
                maximumMessageLag()
                    .thenApply(max -> Optional.of(status.withMaximumMessageLag(max))))
        .update(
            status ->
                messageLag(
                        kafkaAdmin,
                        group ->
                            status.maximumMessageLagGroups.contains(removeSuffix(group, context)))
                    .thenApply(lag -> Optional.of(status.withMessageLag(removeSuffixes(lag)))))
        .update(
            status ->
                completedFuture(
                    Optional.of(status.withInstanceCapacityPerSecond(instanceCapacityPerSecond()))))
        .build();
  }

  private static class Status {
    private final Set<String> allApplications;
    private final long instanceCapacityPerSecond;
    private final Map<String, Integer> maximumAllowed;
    private final Set<String> maximumMessageLagGroups;
    private final Map<String, Map<String, Long>> maximumMessageLagPerGroup;
    private final Map<String, Map<TopicPartition, Long>> messageLagPerGroup;
    private final Map<String, Set<String>> runningInstancesPerApplication;

    private Status() {
      this(null, null, null, null, 0L);
    }

    private Status(
        final Set<String> allApplications,
        final Map<String, Set<String>> runningInstancesPerApplication,
        final Map<String, Map<String, Long>> maximumMessageLagPerGroup,
        final Map<String, Map<TopicPartition, Long>> messageLagPerGroup,
        final long instanceCapacityPerSecond) {
      this.allApplications = allApplications;
      this.runningInstancesPerApplication = runningInstancesPerApplication;
      this.maximumMessageLagPerGroup = maximumMessageLagPerGroup;
      this.messageLagPerGroup = messageLagPerGroup;
      this.instanceCapacityPerSecond = instanceCapacityPerSecond;
      maximumAllowed = ofNullable(messageLagPerGroup).map(Status::maximumAllowed).orElse(null);
      maximumMessageLagGroups =
          ofNullable(maximumMessageLagPerGroup).map(Map::keySet).orElseGet(Collections::emptySet);
    }

    private static int largestTopicSize(final Map<String, Integer> partitionsPerTopic) {
      return partitionsPerTopic.values().stream().max(comparing(v -> v)).orElse(0);
    }

    private static Map<String, Integer> maximumAllowed(
        final Map<String, Map<TopicPartition, Long>> messageLagPerGroup) {
      return messageLagPerGroup.entrySet().stream()
          .map(
              e ->
                  pair(
                      e.getKey(),
                      max(1, largestTopicSize(partitionsPerTopic(e.getValue().keySet().stream())))))
          .collect(toMap(pair -> pair.first, pair -> pair.second));
    }

    private static Map<String, Map<String, Long>> maximumMessageLagPerGroup(
        final JsonObject maximumMessageLagPerGroup) {
      return maximumMessageLagPerGroup.entrySet().stream()
          .filter(e -> isObject(e.getValue()))
          .map(e -> pair(e.getKey(), e.getValue().asJsonObject()))
          .map(pair -> pair(pair.first, maximumMessageLagPerTopic(pair.second)))
          .collect(toMap(pair -> pair.first, pair -> pair.second));
    }

    private static Map<String, Long> maximumMessageLagPerTopic(
        final JsonObject maximumLagPerTopic) {
      return maximumLagPerTopic.entrySet().stream()
          .filter(e -> isLong(e.getValue()))
          .map(e -> pair(e.getKey(), asLong(e.getValue())))
          .collect(toMap(p -> p.first, p -> p.second));
    }

    private static Map<String, Long> messageLagPerTopic(final Map<TopicPartition, Long> lag) {
      return lag.entrySet().stream()
          .collect(groupingBy(e -> e.getKey().topic(), summingLong(Entry::getValue)));
    }

    private static Map<String, Integer> partitionsPerTopic(
        final Stream<TopicPartition> partitions) {
      return partitions.collect(groupingBy(TopicPartition::topic, summingInt(p -> 1)));
    }

    private static boolean tooMuchMessageLag(
        final Map<String, Long> lagPerTopic, final Map<String, Long> maximumLag) {
      return lagPerTopic.entrySet().stream()
          .anyMatch(e -> tooMuchMessageLag(e.getKey(), e.getValue(), maximumLag));
    }

    private static boolean tooMuchMessageLag(
        final String topic, final long lag, final Map<String, Long> maximumLag) {
      return lag > ofNullable(maximumLag.get(topic)).orElse(MAX_VALUE);
    }

    private static long totalExcessMessageLag(
        final Map<String, Long> lagPerTopic, final Map<String, Long> maximumLag) {
      return lagPerTopic.entrySet().stream()
          .filter(e -> maximumLag.containsKey(e.getKey()))
          .mapToLong(e -> e.getValue() - maximumLag.get(e.getKey()))
          .sum();
    }

    private long cap(final long excessMessageLag, final int maximumInstancesAllowed) {
      return min(excessMessageLag, maximumInstancesAllowed * instanceCapacityPerSecond);
    }

    private boolean canRunMore(
        final String application, final Map<String, Integer> maximumAllowed) {
      return ofNullable(runningInstancesPerApplication.get(application))
          .flatMap(r -> ofNullable(maximumAllowed.get(application)).map(m -> r.size() < m))
          .orElse(true);
    }

    private Map<String, Long> excessMessageLag() {
      return messageLagPerGroup()
          .map(
              pair ->
                  pair(
                      pair.first,
                      totalExcessMessageLag(
                          pair.second, maximumMessageLagPerGroup.get(pair.first))))
          .collect(toMap(pair -> pair.first, pair -> pair.second));
    }

    private Stream<Pair<String, Map<String, Long>>> messageLagPerGroup() {
      return messageLagPerGroup.entrySet().stream()
          .filter(e -> canRunMore(e.getKey(), maximumAllowed))
          .map(e -> pair(e.getKey(), messageLagPerTopic(e.getValue())));
    }

    private Set<String> notRunning() {
      return difference(allApplications, runningInstancesPerApplication.keySet());
    }

    private Set<String> runningInstances() {
      return runningInstancesPerApplication.values().stream().flatMap(Set::stream).collect(toSet());
    }

    private Set<String> runningInExcess() {
      return runningInstancesPerApplication.keySet().stream()
          .filter(application -> runningInExcess(application, maximumAllowed))
          .collect(toSet());
    }

    private boolean runningInExcess(
        final String application, final Map<String, Integer> maximumAllowed) {
      return ofNullable(runningInstancesPerApplication.get(application))
          .flatMap(r -> ofNullable(maximumAllowed.get(application)).map(m -> r.size() > m))
          .orElse(false);
    }

    private Set<String> tooMuchMessageLag() {
      return messageLagPerGroup()
          .filter(pair -> tooMuchMessageLag(pair.second, maximumMessageLagPerGroup.get(pair.first)))
          .map(pair -> pair.first)
          .collect(toSet());
    }

    private Status withAllApplications(final Set<String> allApplications) {
      return new Status(
          allApplications,
          runningInstancesPerApplication,
          maximumMessageLagPerGroup,
          messageLagPerGroup,
          instanceCapacityPerSecond);
    }

    private Status withInstanceCapacityPerSecond(final long instanceCapacityPerSecond) {
      return new Status(
          allApplications,
          runningInstancesPerApplication,
          maximumMessageLagPerGroup,
          messageLagPerGroup,
          instanceCapacityPerSecond);
    }

    private Status withMaximumMessageLag(final JsonObject maximumMessageLag) {
      return new Status(
          allApplications,
          runningInstancesPerApplication,
          maximumMessageLagPerGroup(maximumMessageLag),
          messageLagPerGroup,
          instanceCapacityPerSecond);
    }

    private Status withMessageLag(final Map<String, Map<TopicPartition, Long>> messageLag) {
      return new Status(
          allApplications,
          runningInstancesPerApplication,
          maximumMessageLagPerGroup,
          messageLag,
          instanceCapacityPerSecond);
    }

    private Status withRunningInstancesPerApplication(
        final Map<String, Set<String>> runningInstancesPerApplication) {
      return new Status(
          allApplications,
          runningInstancesPerApplication,
          maximumMessageLagPerGroup,
          messageLagPerGroup,
          instanceCapacityPerSecond);
    }
  }
}
