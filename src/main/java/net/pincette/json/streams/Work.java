package net.pincette.json.streams;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Projections.include;
import static java.lang.Integer.parseInt;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
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
import static net.pincette.jes.JsonFields.ID;
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
import static net.pincette.json.JsonUtil.strings;
import static net.pincette.json.streams.Common.ALIVE_AT;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.LEADER;
import static net.pincette.json.streams.Common.config;
import static net.pincette.json.streams.Common.removeSuffix;
import static net.pincette.json.streams.Logging.LOGGER;
import static net.pincette.json.streams.Logging.trace;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Collection.updateOne;
import static net.pincette.mongo.JsonClient.aggregate;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Collections.difference;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Collections.union;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.zip;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.must;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.json.JsonUtil;
import net.pincette.util.AsyncBuilder;
import net.pincette.util.Pair;
import org.bson.Document;

class Work<T, U, V, W> {
  private static final String AVERAGE_MESSAGE_TIME_ESTIMATE = "work.averageMessageTimeEstimate";
  private static final String AVERAGE_MESSAGE_TIME_ESTIMATE_SIM = "averageMessageTimeEstimate";
  private static final Duration DEFAULT_AVERAGE_MESSAGE_TIME_ESTIMATE = ofMillis(20);
  private static final Duration DEFAULT_INTERVAL = ofMinutes(1);
  private static final String DESIRED = "desired";
  private static final int DEFAULT_MAXIMUM_APPS_PER_INSTANCE = 50;
  private static final String EXCESS_MESSAGE_LAG_TOPIC = "work.excessMessageLagTopic";
  private static final String INSTANCES_TOPIC = "work.instancesTopic";
  private static final String INTERVAL = "work.interval";
  private static final String LEADER_FIELD = "leader";
  private static final String MAXIMUM_APPS_PER_INSTANCE = "work.maximumAppsPerInstance";
  private static final String MAXIMUM_APPS_PER_INSTANCE_SIM = "maximumAppsPerInstance";
  private static final String MAXIMUM_MESSAGE_LAG = "maximumMessageLag";
  private static final String MESSAGE_LAG_PER_APPLICATION = "messageLagPerApplication";
  private static final String RUNNING = "running";
  private static final String RUNNING_INSTANCES_WITH_APPLICATIONS =
      "runningInstancesWithApplications";
  private static final String TIME = "time";
  private static final String WORK_LOGGER = LOGGER + ".work";

  private final MongoCollection<Document> collection;
  private final Context context;
  private final String excessMessageLagTopic;
  private final String instancesTopic;
  private final Duration intervalValue;
  private final Provider<T, U, V, W> provider;
  private final WorkContext workContext;
  private Instant lastWork;

  Work(
      final MongoCollection<Document> collection,
      final Provider<T, U, V, W> provider,
      final Context context) {
    this.collection = collection;
    this.provider = provider;
    this.workContext = new WorkContext(context);
    this.context = context;
    this.excessMessageLagTopic =
        config(context, config -> config.getString(EXCESS_MESSAGE_LAG_TOPIC), null);
    this.instancesTopic = config(context, config -> config.getString(INSTANCES_TOPIC), null);
    this.intervalValue = config(context, config -> config.getDuration(INTERVAL), DEFAULT_INTERVAL);
  }

  private static Stream<String> applications(final List<JsonObject> list, final String pointer) {
    return list.stream()
        .map(json -> getString(json, pointer).orElse(null))
        .filter(Objects::nonNull);
  }

  private static long capacityPerSecond(final WorkContext context) {
    return 1000 / context.averageMessageTimeEstimate.toMillis();
  }

  private static Map<String, Set<String>> copy(
      final Map<String, Set<String>> map, final Predicate<String> filter) {
    return map(
        map.entrySet().stream()
            .map(e -> pair(e.getKey(), filterApplications(e.getValue(), filter))));
  }

  private static Map<String, Integer> desiredApplicationInstances(
      final Status status, final WorkContext context) {
    return map(
        status.allApplications.stream()
            .map(app -> pair(app, desiredApplicationInstances(status, app, context))));
  }

  private static int desiredInstances(
      final Map<String, Integer> desiredApplicationInstances, final WorkContext context) {
    final int minimal =
        desiredApplicationInstances.values().stream().max(comparing(v -> v)).orElse(1);
    final int applicationInstances =
        desiredApplicationInstances.values().stream().mapToInt(v -> v).sum();

    return max(
        minimal,
        applicationInstances / context.maximumAppsPerInstance
            + (applicationInstances % context.maximumAppsPerInstance != 0 ? 1 : 0));
  }

  private static int desiredApplicationInstances(
      final Status status, final String application, final WorkContext context) {
    final Map<String, Long> maximumMessageLagPerTopic =
        status.maximumMessageLagPerApplication.get(application);

    return min(
        status.maximumAllowedApplicationInstances(application),
        status.messageLagPerTopic(application).entrySet().stream()
            .map(
                e ->
                    ofNullable(maximumMessageLagPerTopic)
                            .map(
                                max ->
                                    excessCapacityForTopic(e.getKey(), e.getValue(), max, context))
                            .orElse(0)
                        + 1)
            .max(comparing(c -> c))
            .orElse(1));
  }

  private static Map<String, Integer> diffApplicationInstances(
      final Map<String, Integer> remove, final Map<String, Integer> from) {
    return map(
        from.entrySet().stream()
            .map(e -> pair(e.getKey(), e.getValue() - ofNullable(remove.get(e.getKey())).orElse(0)))
            .filter(pair -> pair.second > 0));
  }

  private static int excessCapacityForTopic(
      final String topic,
      final long messageLag,
      final Map<String, Long> maximumMessageLagPerTopic,
      final WorkContext context) {
    return ofNullable(maximumMessageLagPerTopic.get(topic))
        .map(max -> messageLag - max)
        .filter(excessLag -> excessLag > 0)
        .map(excessLag -> extraApplicationCapacity(excessLag, context))
        .orElse(0);
  }

  private static int extraApplicationCapacity(final long messageLag, final WorkContext context) {
    final long capacity = instanceCapacityPerSecond(context);

    return (int) (messageLag / capacity) + (messageLag > 0 && messageLag % capacity != 0 ? 1 : 0);
  }

  private static Set<String> filterApplications(
      final Set<String> applications, final Predicate<String> filter) {
    return applications.stream().filter(filter).collect(toSet());
  }

  private static Map<String, Set<String>> giveWork(
      final Status status,
      final Map<String, Integer> desiredApplicationInstances,
      final WorkContext context) {
    final Map<String, Set<String>> desiredApplicationsPerInstance =
        copy(status.runningInstancesWithApplications, status.allApplications::contains);

    removeRunningInExcess(
        desiredApplicationsPerInstance,
        diffApplicationInstances(
            desiredApplicationInstances, status.runningApplicationInstances()));

    spreadAdditionalApplications(
        desiredApplicationsPerInstance,
        diffApplicationInstances(status.runningApplicationInstances(), desiredApplicationInstances),
        context.maximumAppsPerInstance);

    rebalance(desiredApplicationsPerInstance);

    return desiredApplicationsPerInstance;
  }

  private static long instanceCapacityPerSecond(final WorkContext context) {
    return capacityPerSecond(context) * context.maximumAppsPerInstance;
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

  private static Map<String, Map<Partition, Long>> messageLagPerApplication(final JsonObject json) {
    return json.entrySet().stream()
        .collect(toMap(Entry::getKey, e -> messageLagPerTopic(e.getValue().asJsonObject())));
  }

  private static Map<Partition, Long> messageLagPerTopic(final JsonObject json) {
    return map(
        json.entrySet().stream()
            .flatMap(
                e ->
                    e.getValue().asJsonObject().entrySet().stream()
                        .map(
                            t ->
                                pair(
                                    new Partition(e.getKey(), parseInt(t.getKey())),
                                    asLong(t.getValue())))));
  }

  private static void rebalance(final Map<String, Set<String>> desired) {
    if (!desired.isEmpty()) {
      final int average = runningApplicationInstances(desired) / desired.size();

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

  private static int runningApplicationInstances(final Map<String, Set<String>> work) {
    return work.values().stream().mapToInt(Set::size).sum();
  }

  private static Map<String, Set<String>> runningInstancesWithApplications(final JsonObject json) {
    return json.entrySet().stream()
        .collect(toMap(Entry::getKey, e -> strings(e.getValue().asJsonArray()).collect(toSet())));
  }

  private static Pair<Integer, Integer> scalingIndicator(
      final Map<String, Set<String>> work,
      final Map<String, Integer> desiredApplicationInstances,
      final WorkContext context) {
    return pair(work.size(), desiredInstances(desiredApplicationInstances, context));
  }

  private static int simulate(final Status status, final WorkContext context) {
    final Map<String, Integer> desiredApplicationInstances =
        desiredApplicationInstances(status, context);

    return scalingIndicator(
            giveWork(status, desiredApplicationInstances, context),
            desiredApplicationInstances,
            context)
        .second;
  }

  static int simulate(final JsonObject json) {
    return simulate(
        new Status()
            .withAllApplications(json.getJsonObject(MESSAGE_LAG_PER_APPLICATION).keySet())
            .withMaximumMessageLag(
                ofNullable(json.getJsonObject(MAXIMUM_MESSAGE_LAG))
                    .orElseGet(JsonUtil::emptyObject))
            .withMessageLagPerApplication(
                messageLagPerApplication(json.getJsonObject(MESSAGE_LAG_PER_APPLICATION)))
            .withRunningInstancesWithApplications(
                runningInstancesWithApplications(
                    json.getJsonObject(RUNNING_INSTANCES_WITH_APPLICATIONS))),
        new WorkContext()
            .withAverageMessageTimeEstimate(
                ofMillis(json.getInt(AVERAGE_MESSAGE_TIME_ESTIMATE_SIM)))
            .withMaximumAppsPerInstance(json.getInt(MAXIMUM_APPS_PER_INSTANCE_SIM)));
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

  /** Some extra application instances may not be scheduled because there is no room left. */
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

  private CompletionStage<Set<String>> allApplications() {
    return aggregate(
            collection, list(match(exists(APPLICATION_FIELD)), project(include(APPLICATION_FIELD))))
        .thenApply(list -> applications(list, "/" + APPLICATION_FIELD).collect(toSet()));
  }

  private boolean canWork() {
    if (lastWork == null || now().isAfter(lastWork.plus(intervalValue))) {
      lastWork = now();

      return true;
    }

    return false;
  }

  private JsonObject createScalingIndicatorMessage(final int running, final int desired) {
    return o(
        f(RUNNING, v(running)),
        f(DESIRED, v(desired)),
        f(ID, v(context.instance)),
        f(TIME, v(now().toString())));
  }

  CompletionStage<Boolean> giveWork() {
    return canWork()
        ? status()
            .thenComposeAsync(
                status ->
                    status
                        .map(s -> pair(s, desiredApplicationInstances(s, workContext)))
                        .map(
                            pair ->
                                pair(giveWork(pair.first, pair.second, workContext), pair.second))
                        .map(pair -> pair(logWork(pair.first), pair.second))
                        .map(pair -> scaling(pair.first, pair.second))
                        .map(this::sendInstances)
                        .map(this::saveWork)
                        .orElseGet(() -> completedFuture(false)))
            .exceptionally(
                e -> {
                  severe(Stream.of(e.getMessage(), getStackTrace(e)));
                  return false;
                })
        : completedFuture(false);
  }

  private void info(final Stream<String> messages) {
    log(INFO, messages);
  }

  private void log(final Level level, final Stream<String> messages) {
    Logger.getLogger(LOGGER)
        .log(
            level,
            () -> "Instance " + context.instance + ":\n  " + messages.collect(joining("\n  ")));
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

  private Map<String, Map<Partition, Long>> removeSuffixes(
      final Map<String, Map<Partition, Long>> messageLagPerApplication) {
    return map(
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
                    new Field<>(DESIRED, fromJson(from(applications.stream().sorted()))))))
        .thenApply(result -> trace("saveWork", result, WORK_LOGGER))
        .thenApply(UpdateResult::wasAcknowledged)
        .thenApply(result -> must(result, r -> r));
  }

  private Map<String, Set<String>> scaling(
      final Map<String, Set<String>> work, final Map<String, Integer> desiredApplicationInstances) {
    final Pair<Integer, Integer> pair =
        scalingIndicator(work, desiredApplicationInstances, workContext);

    sendExcessMessageLag(pair.first, pair.second);

    return work;
  }

  private void sendExcessMessageLag(final int running, final int desired) {
    ofNullable(excessMessageLagTopic)
        .ifPresent(
            topic ->
                context.producer.sendJson(
                    topic,
                    message(context.instance, createScalingIndicatorMessage(running, desired))));
  }

  private Map<String, Set<String>> sendInstances(final Map<String, Set<String>> work) {
    return ofNullable(instancesTopic)
        .map(topic -> pair(topic, instances(context.instance, work)))
        .map(
            pair ->
                context.producer.sendJson(
                    pair.first, message(pair.second.getString(ID), pair.second)))
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
                provider
                    .messageLag(
                        group ->
                            status.maximumMessageLagApplications.contains(
                                removeSuffix(group, context)))
                    .thenApply(
                        lag ->
                            Optional.of(status.withMessageLagPerApplication(removeSuffixes(lag)))))
        .build();
  }

  private static class Status {
    private final Set<String> allApplications;
    private final Set<String> maximumMessageLagApplications;
    private final Map<String, Map<String, Long>> maximumMessageLagPerApplication;
    private final Map<String, Map<Partition, Long>> messageLagPerApplication;
    private final Map<String, Set<String>> runningInstancesWithApplications;

    private Status() {
      this(null, null, null, null);
    }

    private Status(
        final Set<String> allApplications,
        final Map<String, Set<String>> runningInstancesWithApplications,
        final Map<String, Map<String, Long>> maximumMessageLagPerApplication,
        final Map<String, Map<Partition, Long>> messageLagPerApplication) {
      this.allApplications = allApplications;
      this.runningInstancesWithApplications = runningInstancesWithApplications;
      this.maximumMessageLagPerApplication = maximumMessageLagPerApplication;
      this.messageLagPerApplication = messageLagPerApplication;
      maximumMessageLagApplications =
          ofNullable(maximumMessageLagPerApplication)
              .map(Map::keySet)
              .orElseGet(Collections::emptySet);
    }

    private static int largestTopicSize(final Map<String, Integer> partitionsPerTopic) {
      return partitionsPerTopic.values().stream().max(comparing(v -> v)).orElse(0);
    }

    private static Map<String, Map<String, Long>> maximumMessageLagPerApplication(
        final JsonObject maximumMessageLagPerApplication) {
      return map(
          maximumMessageLagPerApplication.entrySet().stream()
              .filter(e -> isObject(e.getValue()))
              .map(e -> pair(e.getKey(), e.getValue().asJsonObject()))
              .map(pair -> pair(pair.first, maximumMessageLagPerTopic(pair.second))));
    }

    private static Map<String, Long> maximumMessageLagPerTopic(
        final JsonObject maximumLagPerTopic) {
      return map(
          maximumLagPerTopic.entrySet().stream()
              .filter(e -> isLong(e.getValue()))
              .map(e -> pair(e.getKey(), asLong(e.getValue()))));
    }

    private static Map<String, Long> messageLagPerTopic(final Map<Partition, Long> messageLag) {
      return messageLag.entrySet().stream()
          .collect(groupingBy(e -> e.getKey().topic, summingLong(Entry::getValue)));
    }

    private static Map<String, Integer> partitionsPerTopic(final Stream<Partition> partitions) {
      return partitions.collect(groupingBy(p -> p.topic, summingInt(p -> 1)));
    }

    private int maximumAllowedApplicationInstances(final String application) {
      return ofNullable(messageLagPerApplication.get(application))
          .map(lag -> largestTopicSize(partitionsPerTopic(lag.keySet().stream())))
          .orElse(1);
    }

    private Map<String, Long> messageLagPerTopic(final String application) {
      return ofNullable(messageLagPerApplication.get(application))
          .map(Status::messageLagPerTopic)
          .orElseGet(Collections::emptyMap);
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
        final Map<String, Map<Partition, Long>> messageLagPerApplication) {
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

  private static class WorkContext {
    private final Duration averageMessageTimeEstimate;
    private final int maximumAppsPerInstance;

    private WorkContext() {
      this(null, -1);
    }

    private WorkContext(
        final Duration averageMessageTimeEstimate, final int maximumAppsPerInstance) {
      this.averageMessageTimeEstimate = averageMessageTimeEstimate;
      this.maximumAppsPerInstance = maximumAppsPerInstance;
    }

    private WorkContext(final Context context) {
      this(averageMessageTimeEstimate(context), maximumAppsPerInstance(context));
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

    private WorkContext withAverageMessageTimeEstimate(final Duration averageMessageTimeEstimate) {
      return new WorkContext(averageMessageTimeEstimate, maximumAppsPerInstance);
    }

    private WorkContext withMaximumAppsPerInstance(final int maximumAppsPerInstance) {
      return new WorkContext(averageMessageTimeEstimate, maximumAppsPerInstance);
    }
  }
}
