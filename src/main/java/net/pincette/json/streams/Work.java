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
import static java.util.logging.Level.INFO;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.summingInt;
import static java.util.stream.Collectors.summingLong;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static net.pincette.config.Util.configValue;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.strings;
import static net.pincette.json.streams.Common.ALIVE_AT;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.LEADER;
import static net.pincette.json.streams.Common.config;
import static net.pincette.json.streams.Common.configValueApp;
import static net.pincette.json.streams.Common.removeSuffix;
import static net.pincette.json.streams.Logging.LOGGER_NAME;
import static net.pincette.json.streams.Logging.getLogger;
import static net.pincette.json.streams.Logging.trace;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Collections.difference;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Collections.union;
import static net.pincette.util.ImmutableBuilder.create;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.concat;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.stream;
import static net.pincette.util.StreamUtil.zip;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.typesafe.config.Config;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.BsonUtil;
import net.pincette.util.Pair;
import org.bson.Document;
import org.bson.conversions.Bson;

class Work<T, U, V, W> {
  private static final String AVERAGE_MESSAGE_TIME_ESTIMATE = "work.averageMessageTimeEstimate";
  private static final String AVERAGE_MESSAGE_TIME_ESTIMATE_SIM = "averageMessageTimeEstimate";
  private static final String COOL_DOWN_PERIOD = "work.coolDownPeriod";
  private static final Duration DEFAULT_AVERAGE_MESSAGE_TIME_ESTIMATE = ofMillis(20);
  private static final Duration DEFAULT_COOL_DOWN_PERIOD = ofMinutes(5);
  private static final Duration DEFAULT_INTERVAL = ofMinutes(1);
  private static final boolean DEFAULT_SCALE_TO_ZERO = true;
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
  private static final String SCALE_TO_ZERO = "work.scaleToZero";
  private static final String TIME = "time";
  private static final Logger WORK_LOGGER = getLogger(LOGGER_NAME + ".work");

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
    this.context = context;
    workContext = new WorkContext(context);
    excessMessageLagTopic =
        config(context, config -> config.getString(EXCESS_MESSAGE_LAG_TOPIC), null);
    instancesTopic = config(context, config -> config.getString(INSTANCES_TOPIC), null);
    intervalValue = ofNullable(context).map(c -> intervalValue(c.config)).orElse(null);
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
    return trace(
        () -> "Desired application instances: ",
        new TreeMap<>(
            map(
                status.allApplications.stream()
                    .map(app -> pair(app, desiredApplicationInstances(status, app, context))))),
        WORK_LOGGER);
  }

  private static int desiredInstances(
      final Map<String, Integer> desiredApplicationInstances, final WorkContext context) {
    final int minimal =
        max(1, desiredApplicationInstances.values().stream().max(comparing(v -> v)).orElse(1));
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
        trace(
            () -> "Maximum allowed instances for " + application + ": ",
            status.maximumAllowedApplicationInstances(application),
            WORK_LOGGER),
        status.messageLagPerTopic(application).entrySet().stream()
            .map(
                e ->
                    ofNullable(maximumMessageLagPerTopic)
                            .map(
                                max ->
                                    excessCapacityForTopic(e.getKey(), e.getValue(), max, context))
                            .orElse(0)
                        + (scaleToZero(application, context.config)
                                && status.totalMessageLag(application) == 0
                                && noActivity(status, application, context)
                            ? 0
                            : 1))
            .max(comparing(c -> c))
            .orElse(1));
  }

  private static Map<String, Integer> diffApplicationInstances(
      final Map<String, Integer> running, final Map<String, Integer> desired) {
    return map(
        desired.entrySet().stream()
            .map(
                e -> pair(e.getKey(), e.getValue() - ofNullable(running.get(e.getKey())).orElse(0)))
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
        .map(
            excessLag ->
                trace(
                    () -> "Required extra application capacity: ",
                    extraApplicationCapacity(
                        trace(
                            () -> "Excess message lag for topic " + topic, excessLag, WORK_LOGGER),
                        context),
                    WORK_LOGGER))
        .orElse(0);
  }

  private static int extraApplicationCapacity(final long messageLag, final WorkContext context) {
    final long capacity =
        trace(() -> "Application capacity per second: ", capacityPerSecond(context), WORK_LOGGER);

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
        diffApplicationInstances(desiredApplicationInstances, status.runningApplicationInstances()),
        context);

    spreadAdditionalApplications(
        desiredApplicationsPerInstance,
        diffApplicationInstances(
            status.runningApplicationInstances(), desiredApplicationInstances));

    rebalance(desiredApplicationsPerInstance);
    warnForExtraApplications(desiredApplicationsPerInstance, context.maximumAppsPerInstance);
    context.removeCold();
    context.addCoolingDown(status.runningInstancesWithApplications, desiredApplicationsPerInstance);

    return desiredApplicationsPerInstance;
  }

  private static boolean hadActivity(final Partition partition, final WorkContext context) {
    return ofNullable(context.offsets.get(partition))
        .map(o -> o.lastChanged().plus(context.coolDownPeriod).isAfter(now()))
        .orElse(false);
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

  private static Duration intervalValue(final Config config) {
    return ofNullable(config)
        .flatMap(c -> configValue(c::getDuration, INTERVAL))
        .orElse(DEFAULT_INTERVAL);
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

  private static boolean noActivity(
      final Status status, final String application, final WorkContext context) {
    return status.messageLagPerApplication.get(application).keySet().stream()
        .noneMatch(partition -> hadActivity(partition, context));
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
      final Map<String, Set<String>> desiredApplicationsPerInstance,
      final Map<String, Integer> runningInExcess,
      final WorkContext context) {
    runningInExcess.forEach(
        (key, value) -> removeRunningInExcess(desiredApplicationsPerInstance, key, value, context));
  }

  private static void removeRunningInExcess(
      final Map<String, Set<String>> desiredApplicationsPerInstance,
      final String application,
      final int excess,
      final WorkContext context) {
    final Set<String> toRemove = set(application);

    zip(
            rangeExclusive(0, excess),
            largestInstancesWithApplication(desiredApplicationsPerInstance, application))
        .filter(pair -> !context.isCoolingDown(application))
        .forEach(
            pair ->
                desiredApplicationsPerInstance.put(
                    pair.second.getKey(), difference(pair.second.getValue(), toRemove)));
  }

  private static int runningApplicationInstances(final Map<String, Set<String>> work) {
    return work.values().stream().mapToInt(Set::size).sum();
  }

  private static Map<String, Set<String>> runningInstancesWithApplications(final JsonObject json) {
    return json.entrySet().stream()
        .collect(toMap(Entry::getKey, e -> strings(e.getValue().asJsonArray()).collect(toSet())));
  }

  private static boolean scaleToZero(final String application, final Config config) {
    return ofNullable(config)
        .flatMap(c -> configValueApp(c::getBoolean, SCALE_TO_ZERO, application))
        .orElse(DEFAULT_SCALE_TO_ZERO);
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
            .withMaximumAppsPerInstance(json.getInt(MAXIMUM_APPS_PER_INSTANCE_SIM))
            .withCoolDownPeriod(ofMillis(0)));
  }

  private static int smallestInstance(
      final Entry<String, Set<String>> e1, final Entry<String, Set<String>> e2) {
    return e1.getValue().size() - e2.getValue().size();
  }

  private static Stream<Entry<String, Set<String>>> smallestInstancesWithoutApplication(
      final Map<String, Set<String>> desiredPerInstance, final String application) {
    return desiredPerInstance.entrySet().stream()
        .filter(e -> !e.getValue().contains(application))
        .sorted(Work::smallestInstance);
  }

  private static void spreadAdditionalApplications(
      final Map<String, Set<String>> desiredPerInstance, final Map<String, Integer> additional) {
    additional.forEach(
        (key, value) -> spreadAdditionalApplications(desiredPerInstance, key, value));
  }

  private static void spreadAdditionalApplications(
      final Map<String, Set<String>> desiredPerInstance,
      final String application,
      final int count) {
    final Set<String> toAdd = set(application);

    zip(
            rangeExclusive(0, count),
            smallestInstancesWithoutApplication(desiredPerInstance, application))
        .forEach(
            pair ->
                desiredPerInstance.put(pair.second.getKey(), union(pair.second.getValue(), toAdd)));
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

  private static void warnForExtraApplications(
      final Map<String, Set<String>> desiredApplicationsPerInstance,
      final int maximumAppsPerInstance) {
    desiredApplicationsPerInstance.forEach(
        (k, v) -> {
          if (v.size() > maximumAppsPerInstance) {
            WORK_LOGGER.warning(
                () ->
                    "Instance "
                        + k
                        + " has "
                        + (v.size() - maximumAppsPerInstance)
                        + " more applications running than its theoretical capacity.");
          }
        });
  }

  private Set<String> allApplications() {
    return stream(
            collection
                .aggregate(
                    list(match(exists(APPLICATION_FIELD)), project(include(APPLICATION_FIELD))))
                .iterator())
        .map(result -> result.getString(APPLICATION_FIELD))
        .collect(toSet());
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

  void giveWork() {
    if (canWork()) {
      final Status status = status();

      workContext.updateOffsets(status.offsets);

      final Map<String, Integer> desired = desiredApplicationInstances(status, workContext);
      final Map<String, Set<String>> desiredPerInstance = giveWork(status, desired, workContext);

      logWork(desiredPerInstance);
      scaling(desiredPerInstance, desired);
      sendInstances(desiredPerInstance);
      saveWork(desiredPerInstance);
    }
  }

  private void info(final Stream<String> messages) {
    WORK_LOGGER.log(
        INFO, () -> "Instance " + context.instance + ":\n  " + messages.collect(joining("\n  ")));
  }

  private void logWork(final Map<String, Set<String>> work) {
    info(
        work.entrySet().stream()
            .map(
                e ->
                    e.getKey()
                        + ": "
                        + e.getValue().stream().sorted().collect(Collectors.joining(", "))));
  }

  private JsonObject maximumMessageLag() {
    return stream(collection.find(exists(MAXIMUM_MESSAGE_LAG)).iterator())
        .findFirst()
        .map(Bson::toBsonDocument)
        .map(BsonUtil::fromBson)
        .map(json -> json.getJsonObject(MAXIMUM_MESSAGE_LAG))
        .orElseGet(JsonUtil::emptyObject);
  }

  private Map<String, Map<Partition, Long>> removeSuffixes(
      final Map<String, Map<Partition, Long>> messageLagPerApplication) {
    return map(
        messageLagPerApplication.entrySet().stream()
            .map(e -> pair(removeSuffix(e.getKey(), context), e.getValue())));
  }

  private Map<String, Set<String>> runningInstancesWithApplications() {
    return stream(
            collection.aggregate(list(match(exists(ALIVE_AT)), match(ne(ID, LEADER)))).iterator())
        .collect(
            toMap(
                result -> result.getString(ID),
                result ->
                    ofNullable(result.getList(DESIRED, String.class))
                        .map(HashSet::new)
                        .orElseGet(HashSet::new)));
  }

  private boolean saveWork(final Map<String, Set<String>> work) {
    return work.entrySet().stream()
        .map(e -> saveWork(e.getKey(), e.getValue()))
        .reduce((r1, r2) -> r1 && r2)
        .orElse(true);
  }

  private boolean saveWork(final String instance, final Set<String> applications) {
    return collection
        .updateOne(
            eq(ID, instance),
            list(
                Aggregates.set(
                    new Field<>(DESIRED, fromJson(from(applications.stream().sorted()))))))
        .wasAcknowledged();
  }

  private void scaling(
      final Map<String, Set<String>> work, final Map<String, Integer> desiredApplicationInstances) {
    final Pair<Integer, Integer> pair =
        scalingIndicator(work, desiredApplicationInstances, workContext);

    sendExcessMessageLag(pair.first, pair.second);
  }

  private void sendExcessMessageLag(final int running, final int desired) {
    ofNullable(excessMessageLagTopic)
        .ifPresent(
            topic ->
                context.producer.sendJson(
                    topic,
                    message(context.instance, createScalingIndicatorMessage(running, desired))));
  }

  private void sendInstances(final Map<String, Set<String>> work) {
    if (instancesTopic != null) {
      final JsonObject instances = instances(context.instance, work);

      context.producer.sendJson(instancesTopic, message(instances.getString(ID), instances));
    }
  }

  private Status status() {
    return create(Status::new)
        .update(status -> status.withAllApplications(allApplications()))
        .update(
            status ->
                status.withRunningInstancesWithApplications(runningInstancesWithApplications()))
        .update(status -> status.withMaximumMessageLag(maximumMessageLag()))
        .update(
            status -> {
              trace(() -> "Fetching message lag at " + now(), null, WORK_LOGGER);

              return trace(
                  () -> "Received message lag at " + now(),
                  provider
                      .messageLag(
                          group -> status.allApplications.contains(removeSuffix(group, context)))
                      .thenApply(lag -> status.withMessageLagPerApplication(removeSuffixes(lag)))
                      .toCompletableFuture()
                      .join(),
                  WORK_LOGGER);
            })
        .update(
            status -> {
              trace(() -> "Fetching offsets at " + now(), null, WORK_LOGGER);

              return trace(
                  () -> "Received offsets at " + now(),
                  provider.offsets().thenApply(status::withOffsets).toCompletableFuture().join(),
                  WORK_LOGGER);
            })
        .build();
  }

  private record OffsetInfo(long offset, Instant lastChanged) {}

  private static class Status {
    private final Set<String> allApplications;
    private final Map<String, Map<String, Long>> maximumMessageLagPerApplication;
    private final Map<String, Map<Partition, Long>> messageLagPerApplication;
    private final Map<Partition, Long> offsets;
    private final Map<String, Set<String>> runningInstancesWithApplications;

    private Status() {
      this(null, null, null, null, null);
    }

    private Status(
        final Set<String> allApplications,
        final Map<String, Set<String>> runningInstancesWithApplications,
        final Map<String, Map<String, Long>> maximumMessageLagPerApplication,
        final Map<String, Map<Partition, Long>> messageLagPerApplication,
        final Map<Partition, Long> offsets) {
      this.allApplications = allApplications;
      this.runningInstancesWithApplications = runningInstancesWithApplications;
      this.maximumMessageLagPerApplication = maximumMessageLagPerApplication;
      this.messageLagPerApplication = messageLagPerApplication;
      this.offsets = offsets;
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
          .collect(groupingBy(e -> e.getKey().topic(), summingLong(Entry::getValue)));
    }

    private static Map<String, Integer> partitionsPerTopic(final Stream<Partition> partitions) {
      return partitions.collect(groupingBy(Partition::topic, summingInt(p -> 1)));
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

    private long totalMessageLag(final String application) {
      return ofNullable(messageLagPerApplication.get(application)).stream()
          .flatMap(m -> m.values().stream())
          .mapToLong(l -> l)
          .sum();
    }

    private Status withAllApplications(final Set<String> allApplications) {
      return new Status(
          allApplications,
          runningInstancesWithApplications,
          maximumMessageLagPerApplication,
          messageLagPerApplication,
          offsets);
    }

    private Status withMaximumMessageLag(final JsonObject maximumMessageLag) {
      return new Status(
          allApplications,
          runningInstancesWithApplications,
          maximumMessageLagPerApplication(maximumMessageLag),
          messageLagPerApplication,
          offsets);
    }

    private Status withMessageLagPerApplication(
        final Map<String, Map<Partition, Long>> messageLagPerApplication) {
      return new Status(
          allApplications,
          runningInstancesWithApplications,
          maximumMessageLagPerApplication,
          messageLagPerApplication,
          offsets);
    }

    private Status withOffsets(final Map<Partition, Long> offsets) {
      return new Status(
          allApplications,
          runningInstancesWithApplications,
          maximumMessageLagPerApplication,
          messageLagPerApplication,
          offsets);
    }

    private Status withRunningInstancesWithApplications(
        final Map<String, Set<String>> runningInstancesWithApplications) {
      return new Status(
          allApplications,
          runningInstancesWithApplications,
          maximumMessageLagPerApplication,
          messageLagPerApplication,
          offsets);
    }
  }

  static class WorkContext {
    private final Duration averageMessageTimeEstimate;
    private final Config config;
    private final Duration coolDownPeriod;
    private final int maximumAppsPerInstance;
    private final Map<Partition, OffsetInfo> offsets = new HashMap<>();
    private final Map<String, Instant> started = new HashMap<>();

    private WorkContext() {
      this(null, -1, null);
    }

    private WorkContext(
        final Duration averageMessageTimeEstimate,
        final int maximumAppsPerInstance,
        final Duration coolDownPeriod) {
      this(averageMessageTimeEstimate, maximumAppsPerInstance, coolDownPeriod, null);
    }

    private WorkContext(
        final Duration averageMessageTimeEstimate,
        final int maximumAppsPerInstance,
        final Duration coolDownPeriod,
        final Config config) {
      this.averageMessageTimeEstimate = averageMessageTimeEstimate;
      this.maximumAppsPerInstance = maximumAppsPerInstance;
      this.coolDownPeriod = coolDownPeriod;
      this.config = config;
    }

    private WorkContext(final Context context) {
      this(
          averageMessageTimeEstimate(context),
          maximumAppsPerInstance(context),
          coolDownPeriod(context),
          context.config);
    }

    private static Stream<String> applicationsForNewInstances(
        final Map<String, Set<String>> running, final Map<String, Set<String>> desired) {
      return desired.entrySet().stream()
          .filter(e -> !running.containsKey(e.getKey()))
          .flatMap(e -> e.getValue().stream());
    }

    private static Stream<String> applicationsForExistingInstances(
        final Map<String, Set<String>> running, final Map<String, Set<String>> desired) {
      return desired.entrySet().stream()
          .flatMap(
              e ->
                  ofNullable(running.get(e.getKey())).stream()
                      .flatMap(apps -> difference(e.getValue(), apps).stream()));
    }

    private static Duration averageMessageTimeEstimate(final Context context) {
      return config(
          context,
          config -> config.getDuration(AVERAGE_MESSAGE_TIME_ESTIMATE),
          DEFAULT_AVERAGE_MESSAGE_TIME_ESTIMATE);
    }

    private static Duration coolDownPeriod(final Context context) {
      return configValue(context.config::getDuration, COOL_DOWN_PERIOD)
          .orElse(DEFAULT_COOL_DOWN_PERIOD);
    }

    static int maximumAppsPerInstance(final Context context) {
      return config(
          context,
          config -> config.getInt(MAXIMUM_APPS_PER_INSTANCE),
          DEFAULT_MAXIMUM_APPS_PER_INSTANCE);
    }

    private void addCoolingDown(
        final Map<String, Set<String>> running, final Map<String, Set<String>> desired) {
      final var now = now();

      concat(
              applicationsForNewInstances(running, desired),
              applicationsForExistingInstances(running, desired))
          .forEach(app -> started.put(app, now));
    }

    private boolean isCoolingDown(final String application) {
      return ofNullable(started.get(application)).map(this::isCoolingDown).orElse(false);
    }

    private boolean isCoolingDown(final Instant moment) {
      return moment.plus(coolDownPeriod).isAfter(now());
    }

    private void removeCold() {
      var keys =
          started.entrySet().stream()
              .filter(e -> !isCoolingDown(e.getValue()))
              .map(Entry::getKey)
              .collect(toSet());

      keys.forEach(started::remove);
    }

    private void updateOffsets(final Map<Partition, Long> fetched) {
      fetched.entrySet().stream()
          .filter(
              e ->
                  ofNullable(offsets.get(e.getKey()))
                      .map(o -> e.getValue() > o.offset)
                      .orElse(true))
          .forEach(e -> offsets.put(e.getKey(), new OffsetInfo(e.getValue(), now())));
    }

    private WorkContext withAverageMessageTimeEstimate(final Duration averageMessageTimeEstimate) {
      return new WorkContext(averageMessageTimeEstimate, maximumAppsPerInstance, coolDownPeriod);
    }

    private WorkContext withCoolDownPeriod(final Duration coolDownPeriod) {
      return new WorkContext(averageMessageTimeEstimate, maximumAppsPerInstance, coolDownPeriod);
    }

    private WorkContext withMaximumAppsPerInstance(final int maximumAppsPerInstance) {
      return new WorkContext(averageMessageTimeEstimate, maximumAppsPerInstance, coolDownPeriod);
    }
  }
}
