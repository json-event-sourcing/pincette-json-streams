package net.pincette.json.streams;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Projections.include;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.time.Duration.ofMillis;
import static java.time.Instant.now;
import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.summingInt;
import static java.util.stream.Collectors.summingLong;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.Kafka.messageLag;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getStrings;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.streams.Common.ALIVE_AT;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.LEADER;
import static net.pincette.json.streams.Common.config;
import static net.pincette.json.streams.Common.removeSuffix;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Collection.updateOne;
import static net.pincette.mongo.JsonClient.aggregate;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.util.Collections.difference;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Collections.union;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.zip;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.must;
import static org.apache.kafka.clients.admin.AdminClient.create;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.json.JsonUtil;
import net.pincette.util.AsyncBuilder;
import net.pincette.util.Pair;
import net.pincette.util.StreamUtil;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.bson.Document;

class Work {
  private static final String AVERAGE_MESSAGE_TIME_ESTIMATE = "work.averageMessageTimeEstimate";
  private static final Duration DEFAULT_AVERAGE_MESSAGE_TIME_ESTIMATE = ofMillis(50);
  private static final String DESIRED = "desired";
  private static final int DEFAULT_MAXIMUM_APPS_PER_INSTANCE = 10;
  private static final String EXCESS_MESSAGE_LAG = "excessMessageLag";
  private static final String EXCESS_MESSAGE_LAG_TOPIC = "work.excessMessageLagTopic";
  private static final String INSTANCES_TOPIC = "work.instancesTopic";
  private static final String LEADER_FIELD = "leader";
  private static final String MAXIMUM_APPS_PER_INSTANCE = "work.maximumAppsPerInstance";
  private static final String MAXIMUM_MESSAGE_LAG = "maximumMessageLag";
  private static final String TIME = "time";

  private final Duration averageMessageTimeEstimate;
  private final MongoCollection<Document> collection;
  private final Context context;
  private final String excessMessageLagTopic;
  private final String instancesTopic;
  private final Admin kafkaAdmin;
  private final int maximumAppsPerInstance;

  Work(
      final MongoCollection<Document> collection,
      final Map<String, Object> kafkaAdminConfig,
      final Context context) {
    this.collection = collection;
    this.kafkaAdmin = create(kafkaAdminConfig);
    this.averageMessageTimeEstimate = averageMessageTimeEstimate(context);
    this.maximumAppsPerInstance = maximumAppsPerInstance(context);
    this.context = context;
    this.excessMessageLagTopic =
        config(context, config -> config.getString(EXCESS_MESSAGE_LAG_TOPIC), null);
    this.instancesTopic = config(context, config -> config.getString(INSTANCES_TOPIC), null);
  }

  private static int applicationInstancesRunning(
      final Map<String, Set<String>> work, final String application) {
    return work.values().stream()
        .mapToInt(applications -> applications.contains(application) ? 1 : 0)
        .sum();
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

  private static Map<String, Set<String>> copy(
      final Map<String, Set<String>> map, final Predicate<String> filter) {
    return StreamUtil.toMap(
        map.entrySet().stream()
            .map(e -> pair(e.getKey(), filterApplications(e.getValue(), filter))));
  }

  private static Set<String> filterApplications(
      final Set<String> applications, final Predicate<String> filter) {
    return applications.stream().filter(filter).collect(toSet());
  }

  private static JsonObject instances(final String leader, final Map<String, Set<String>> work) {
    return work.entrySet().stream()
        .reduce(
            createObjectBuilder()
                .add(ID, randomUUID().toString())
                .add(TIME, now().toString())
                .add(LEADER_FIELD, leader),
            (b, e) -> b.add(e.getKey(), from(e.getValue().stream())),
            (b1, b2) -> b1)
        .build();
  }

  private static int keepAtLeastOne(
      final String application, final Status status, final int extraCapacity) {
    return extraCapacity < 0
        ? max(
            extraCapacity,
            applicationInstancesRunning(status.runningInstancesWithApplications, application) - 1)
        : extraCapacity;
  }

  private static int largestInstance(
      final Entry<String, Set<String>> e1, final Entry<String, Set<String>> e2) {
    return e2.getValue().size() - e1.getValue().size();
  }

  private static Stream<Entry<String, Set<String>>> largestInstancesWithApplication(
      final Map<String, Set<String>> desired, final String application) {
    return desired.entrySet().stream()
        .filter(e -> e.getValue().contains(application))
        .sorted(Work::largestInstance);
  }

  private static int maximumAppsPerInstance(final Context context) {
    return config(
        context,
        config -> config.getInt(MAXIMUM_APPS_PER_INSTANCE),
        DEFAULT_MAXIMUM_APPS_PER_INSTANCE);
  }

  private static void rebalance(final Map<String, Set<String>> desired) {
    if (!desired.isEmpty()) {
      final int average = desired.values().stream().mapToInt(Set::size).sum() / desired.size();

      if (average > 0) {
        desired.entrySet().stream()
            .filter(e -> e.getValue().size() > average)
            .sorted(Work::largestInstance)
            .forEach(
                above ->
                    desired.entrySet().stream()
                        .filter(e -> e.getValue().size() < average)
                        .sorted(Work::smallestInstance)
                        .forEach(below -> transfer(above.getValue(), below.getValue(), average)));
      }
    }
  }

  private static void removeRunningInExcess(
      final Map<String, Set<String>> desired, final Map<String, Integer> runningInExcess) {
    runningInExcess.forEach((key, value) -> removeRunningInExcess(desired, key, value));
  }

  private static void removeRunningInExcess(
      final Map<String, Set<String>> desired, final String application, final int excess) {
    final Set<String> toRemove = set(application);

    zip(rangeExclusive(0, excess), largestInstancesWithApplication(desired, application))
        .forEach(
            pair ->
                desired.put(pair.second.getKey(), difference(pair.second.getValue(), toRemove)));
  }

  private static Set<String> runningApplications(final Map<String, Set<String>> work) {
    return work.values().stream().flatMap(Set::stream).collect(toSet());
  }

  private static <K, V> Map<K, V> select(final Map<K, V> map, final BiPredicate<K, V> predicate) {
    return map.entrySet().stream()
        .filter(e -> predicate.test(e.getKey(), e.getValue()))
        .collect(toMap(Entry::getKey, Entry::getValue));
  }

  private static <K> Map<K, Integer> singleOccurrence(final Set<K> values) {
    return values.stream().collect(toMap(v -> v, v -> 1));
  }

  private static int smallestInstance(
      final Entry<String, Set<String>> e1, final Entry<String, Set<String>> e2) {
    return e1.getValue().size() - e2.getValue().size();
  }

  private static Stream<Entry<String, Set<String>>> smallestInstancesWithoutApplication(
      final Map<String, Set<String>> desired,
      final String application,
      final int maximumAppsPerInstance) {
    return desired.entrySet().stream()
        .filter(
            e ->
                !e.getValue().contains(application) && e.getValue().size() < maximumAppsPerInstance)
        .sorted(Work::smallestInstance);
  }

  private static void spreadAdditionalApplications(
      final Map<String, Set<String>> desired,
      final Map<String, Integer> additional,
      final int maximumAppsPerInstance) {
    additional.forEach(
        (key, value) -> spreadAdditionalApplications(desired, key, value, maximumAppsPerInstance));
  }

  private static void spreadAdditionalApplications(
      final Map<String, Set<String>> desired,
      final String application,
      final int count,
      final int maximumAppsPerInstance) {
    final Set<String> toAdd = set(application);

    zip(
            rangeExclusive(0, count),
            smallestInstancesWithoutApplication(desired, application, maximumAppsPerInstance))
        .forEach(pair -> desired.put(pair.second.getKey(), union(pair.second.getValue(), toAdd)));
  }

  private static void transfer(
      final Set<String> above, final Set<String> below, final int average) {
    zip(
            rangeExclusive(0, min(above.size() - average, average - below.size())),
            difference(above, below).stream())
        .map(pair -> pair.second)
        .forEach(
            application -> {
              above.remove(application);
              below.add(application);
            });
  }

  private static Set<String> notRunningApplications(
      final Set<String> allApplications, final Map<String, Set<String>> work) {
    return difference(allApplications, runningApplications(work));
  }

  private CompletionStage<Set<String>> allApplications() {
    return aggregate(
            collection, list(match(exists(APPLICATION_FIELD)), project(include(APPLICATION_FIELD))))
        .thenApply(list -> applications(list, "/" + APPLICATION_FIELD).collect(toSet()));
  }

  private long capacityPerSecond() {
    return 1000 / averageMessageTimeEstimate.toMillis();
  }

  private long excessCapacity(final Map<String, Set<String>> work) {
    return (work.size() - minimumInstanceCapacity(work))
        * maximumAppsPerInstance
        * capacityPerSecond();
  }

  private int extraApplicationCapacity(final long messageLag) {
    final long capacity = instanceCapacityPerSecond();

    return (int) (messageLag / capacity) + (messageLag > 0 && messageLag % capacity != 0 ? 1 : 0);
  }

  CompletionStage<Boolean> giveWork() {
    return status()
        .thenComposeAsync(
            status ->
                status
                    .map(this::giveWork)
                    .map(this::logWork)
                    .map(work -> sendExcessMessageLag(status.get(), work))
                    .map(this::sendInstances)
                    .map(this::saveWork)
                    .orElseGet(() -> completedFuture(false)))
        .exceptionally(
            e -> {
              severe(Stream.of(e.getMessage(), getStackTrace(e)));
              return false;
            });
  }

  private Map<String, Set<String>> giveWork(final Status status) {
    final Map<String, Set<String>> desiredApplicationsPerInstance =
        copy(status.runningInstancesWithApplications, status.allApplications::contains);
    final Map<String, Integer> extraInstances = neededExtraApplicationInstances(status);

    removeRunningInExcess(
        desiredApplicationsPerInstance,
        merge(status.runningInExcess(), select(extraInstances, (k, v) -> v < 0)));

    spreadAdditionalApplications(
        desiredApplicationsPerInstance,
        merge(
            singleOccurrence(status.notRunningApplications()),
            select(extraInstances, (k, v) -> v > 0)),
        maximumAppsPerInstance);

    rebalance(desiredApplicationsPerInstance);

    return desiredApplicationsPerInstance;
  }

  private void info(final Stream<String> messages) {
    log(INFO, messages);
  }

  private long instanceCapacityPerSecond() {
    return capacityPerSecond() * maximumAppsPerInstance;
  }

  private void log(final Level level, final Stream<String> messages) {
    context.logger.log(
        level, () -> "Instance " + context.instance + ":\n  " + messages.collect(joining("\n  ")));
  }

  private Map<String, Set<String>> logWork(final Map<String, Set<String>> work) {
    info(
        work.entrySet().stream()
            .map(
                e ->
                    e.getKey()
                        + ": "
                        + e.getValue().stream().sorted().collect(Collectors.joining(", "))));

    return work;
  }

  private CompletionStage<JsonObject> maximumMessageLag() {
    return findOne(collection, exists(MAXIMUM_MESSAGE_LAG))
        .thenApply(
            result ->
                result
                    .map(r -> r.getJsonObject(MAXIMUM_MESSAGE_LAG))
                    .orElseGet(JsonUtil::emptyObject));
  }

  private long minimumInstanceCapacity(final Map<String, Set<String>> work) {
    return (work.values().stream().mapToLong(Set::size).sum() + maximumAppsPerInstance - 1)
        / maximumAppsPerInstance;
  }

  private long missingCapacity(final Status status, final Map<String, Set<String>> work) {
    return notRunningApplications(status.allApplications, work).size() * capacityPerSecond();
  }

  private Map<String, Integer> neededExtraApplicationInstances(final Status status) {
    return StreamUtil.toMap(
        status.accountableExcessMessageLag().entrySet().stream()
            .map(
                e ->
                    pair(
                        e.getKey(),
                        keepAtLeastOne(
                            e.getKey(), status, extraApplicationCapacity(e.getValue())))));
  }

  private Map<String, Map<TopicPartition, Long>> removeSuffixes(
      final Map<String, Map<TopicPartition, Long>> messageLagPerApplication) {
    return StreamUtil.toMap(
        messageLagPerApplication.entrySet().stream()
            .map(e -> pair(removeSuffix(e.getKey(), context), e.getValue())));
  }

  private CompletionStage<Map<String, Set<String>>> runningInstancesWithApplications() {
    return aggregate(collection, list(match(exists(ALIVE_AT)), match(ne(ID, LEADER))))
        .thenApply(
            list ->
                list.stream()
                    .collect(
                        toMap(
                            json -> json.getString(ID),
                            json -> getStrings(json, DESIRED).collect(toSet()))));
  }

  private CompletionStage<Boolean> saveWork(final Map<String, Set<String>> work) {
    return composeAsyncStream(work.entrySet().stream().map(e -> saveWork(e.getKey(), e.getValue())))
        .thenApply(results -> results.reduce((r1, r2) -> r1 && r2).orElse(true));
  }

  private CompletionStage<Boolean> saveWork(final String instance, final Set<String> applications) {
    return updateOne(
            collection,
            eq(ID, instance),
            list(
                Aggregates.set(
                    new Field<>(DESIRED, fromJson(from(new ArrayList<>(applications)))))))
        .thenApply(UpdateResult::wasAcknowledged)
        .thenApply(result -> must(result, r -> r));
  }

  private Map<String, Set<String>> sendExcessMessageLag(
      final Status status, final Map<String, Set<String>> work) {
    return ofNullable(excessMessageLagTopic)
        .map(
            topic ->
                send(
                    context.producer,
                    new ProducerRecord<>(
                        topic,
                        context.instance,
                        o(
                            f(EXCESS_MESSAGE_LAG, v(totalExcessMessageLag(status, work))),
                            f(ID, v(context.instance)),
                            f(TIME, v(now().toString()))))))
        .map(result -> work)
        .orElse(work);
  }

  private Map<String, Set<String>> sendInstances(final Map<String, Set<String>> work) {
    return ofNullable(instancesTopic)
        .map(topic -> pair(topic, instances(context.instance, work)))
        .map(
            pair ->
                send(
                    context.producer,
                    new ProducerRecord<>(pair.first, pair.second.getString(ID), pair.second)))
        .map(result -> work)
        .orElse(work);
  }

  private void severe(final Stream<String> messages) {
    log(SEVERE, messages);
  }

  private CompletionStage<Optional<Status>> status() {
    return AsyncBuilder.create(Status::new)
        .update(
            status ->
                allApplications().thenApply(all -> Optional.of(status.withAllApplications(all))))
        .update(
            status ->
                runningInstancesWithApplications()
                    .thenApply(
                        running ->
                            Optional.of(status.withRunningInstancesWithApplications(running))))
        .update(
            status ->
                maximumMessageLag()
                    .thenApply(max -> Optional.of(status.withMaximumMessageLag(max))))
        .update(
            status ->
                messageLag(
                        kafkaAdmin,
                        group ->
                            status.maximumMessageLagApplications.contains(
                                removeSuffix(group, context)))
                    .thenApply(
                        lag ->
                            Optional.of(status.withMessageLagPerApplication(removeSuffixes(lag)))))
        .build();
  }

  private long totalExcessMessageLag(final Status status, final Map<String, Set<String>> work) {
    return status.totalExcessMessageLag() + missingCapacity(status, work) - excessCapacity(work);
  }

  private static class Status {
    private final Set<String> allApplications;
    private final Map<String, Integer> maximumAllowed;
    private final Set<String> maximumMessageLagApplications;
    private final Map<String, Map<String, Long>> maximumMessageLagPerApplication;
    private final Map<String, Map<TopicPartition, Long>> messageLagPerApplication;
    private final Map<String, Set<String>> runningInstancesWithApplications;

    private Status() {
      this(null, null, null, null);
    }

    private Status(
        final Set<String> allApplications,
        final Map<String, Set<String>> runningInstancesWithApplications,
        final Map<String, Map<String, Long>> maximumMessageLagPerApplication,
        final Map<String, Map<TopicPartition, Long>> messageLagPerApplication) {
      this.allApplications = allApplications;
      this.runningInstancesWithApplications = runningInstancesWithApplications;
      this.maximumMessageLagPerApplication = maximumMessageLagPerApplication;
      this.messageLagPerApplication = messageLagPerApplication;
      maximumAllowed =
          ofNullable(messageLagPerApplication).map(Status::maximumAllowed).orElse(null);
      maximumMessageLagApplications =
          ofNullable(maximumMessageLagPerApplication)
              .map(Map::keySet)
              .orElseGet(Collections::emptySet);
    }

    private static int largestTopicSize(final Map<String, Integer> partitionsPerTopic) {
      return partitionsPerTopic.values().stream().max(comparing(v -> v)).orElse(0);
    }

    private static Map<String, Integer> maximumAllowed(
        final Map<String, Map<TopicPartition, Long>> messageLagPerApplication) {
      return StreamUtil.toMap(
          messageLagPerApplication.entrySet().stream()
              .map(
                  e ->
                      pair(
                          e.getKey(),
                          max(
                              1,
                              largestTopicSize(
                                  partitionsPerTopic(e.getValue().keySet().stream()))))));
    }

    private static Map<String, Map<String, Long>> maximumMessageLagPerApplication(
        final JsonObject maximumMessageLagPerApplication) {
      return StreamUtil.toMap(
          maximumMessageLagPerApplication.entrySet().stream()
              .filter(e -> isObject(e.getValue()))
              .map(e -> pair(e.getKey(), e.getValue().asJsonObject()))
              .map(pair -> pair(pair.first, maximumMessageLagPerTopic(pair.second))));
    }

    private static Map<String, Long> maximumMessageLagPerTopic(
        final JsonObject maximumLagPerTopic) {
      return StreamUtil.toMap(
          maximumLagPerTopic.entrySet().stream()
              .filter(e -> isLong(e.getValue()))
              .map(e -> pair(e.getKey(), asLong(e.getValue()))));
    }

    private static Map<String, Long> messageLagPerTopic(final Map<TopicPartition, Long> lag) {
      return lag.entrySet().stream()
          .collect(groupingBy(e -> e.getKey().topic(), summingLong(Entry::getValue)));
    }

    private static Map<String, Integer> partitionsPerTopic(
        final Stream<TopicPartition> partitions) {
      return partitions.collect(groupingBy(TopicPartition::topic, summingInt(p -> 1)));
    }

    private static long totalExcessMessageLag(
        final Map<String, Long> lagPerTopic, final Map<String, Long> maximumLag) {
      return lagPerTopic.entrySet().stream()
          .map(e -> pair(e.getValue(), maximumLag.get(e.getKey())))
          .filter(pair -> pair.second != null)
          .mapToLong(pair -> max(pair.first, pair.second) - pair.second)
          .sum();
    }

    private Map<String, Long> accountableExcessMessageLag() {
      return StreamUtil.toMap(excessMessageLag(accountableMessageLagPerApplication()));
    }

    private Stream<Pair<String, Map<String, Long>>> accountableMessageLagPerApplication() {
      return messageLagPerApplication().filter(pair -> canRunMore(pair.first, maximumAllowed));
    }

    private boolean canRunMore(
        final String application, final Map<String, Integer> maximumAllowed) {
      return ofNullable(maximumAllowed.get(application))
          .map(m -> runningApplicationInstances(application) < m)
          .orElse(true);
    }

    private Stream<Pair<String, Long>> excessMessageLag(
        final Stream<Pair<String, Map<String, Long>>> messageLagPerApplication) {
      return messageLagPerApplication.map(
          pair ->
              pair(
                  pair.first,
                  totalExcessMessageLag(
                      pair.second, maximumMessageLagPerApplication.get(pair.first))));
    }

    private Stream<Pair<String, Map<String, Long>>> messageLagPerApplication() {
      return messageLagPerApplication.entrySet().stream()
          .map(e -> pair(e.getKey(), messageLagPerTopic(e.getValue())));
    }

    private Set<String> notRunningApplications() {
      return Work.notRunningApplications(allApplications, runningInstancesWithApplications);
    }

    private Map<String, Integer> runningApplicationInstances() {
      return runningInstancesWithApplications.values().stream()
          .flatMap(Set::stream)
          .reduce(
              new HashMap<>(),
              (m, a) -> {
                m.put(a, m.computeIfAbsent(a, k -> 0) + 1);
                return m;
              },
              (m1, m2) -> m1);
    }

    private long runningApplicationInstances(final String application) {
      return runningInstancesWithApplications.values().stream()
          .filter(v -> v.contains(application))
          .count();
    }

    private Map<String, Integer> runningInExcess() {
      return StreamUtil.toMap(
          runningApplicationInstances().entrySet().stream()
              .filter(e -> maximumAllowed.containsKey(e.getKey()))
              .map(e -> pair(e.getKey(), e.getValue() - maximumAllowed.get(e.getKey())))
              .filter(pair -> pair.second > 0));
    }

    private long totalExcessMessageLag() {
      return excessMessageLag(messageLagPerApplication()).mapToLong(pair -> pair.second).sum();
    }

    private Status withAllApplications(final Set<String> allApplications) {
      return new Status(
          allApplications,
          runningInstancesWithApplications,
          maximumMessageLagPerApplication,
          messageLagPerApplication);
    }

    private Status withMaximumMessageLag(final JsonObject maximumMessageLag) {
      return new Status(
          allApplications,
          runningInstancesWithApplications,
          maximumMessageLagPerApplication(maximumMessageLag),
          messageLagPerApplication);
    }

    private Status withMessageLagPerApplication(
        final Map<String, Map<TopicPartition, Long>> messageLagPerApplication) {
      return new Status(
          allApplications,
          runningInstancesWithApplications,
          maximumMessageLagPerApplication,
          messageLagPerApplication);
    }

    private Status withRunningInstancesWithApplications(
        final Map<String, Set<String>> runningInstancesWithApplications) {
      return new Status(
          allApplications,
          runningInstancesWithApplications,
          maximumMessageLagPerApplication,
          messageLagPerApplication);
    }
  }
}
