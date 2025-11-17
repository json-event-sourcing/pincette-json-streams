package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP;
import static java.lang.Boolean.TRUE;
import static java.lang.Integer.MAX_VALUE;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static net.pincette.config.Util.configValue;
import static net.pincette.jes.Aggregate.reducer;
import static net.pincette.jes.Command.hasError;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.Util.compose;
import static net.pincette.jes.Util.getUsername;
import static net.pincette.jes.tel.OtelUtil.addLabels;
import static net.pincette.jes.tel.OtelUtil.counter;
import static net.pincette.jes.tel.OtelUtil.retainTraceSample;
import static net.pincette.jes.util.Mongo.resolve;
import static net.pincette.jes.util.Mongo.unresolve;
import static net.pincette.jes.util.Mongo.withResolver;
import static net.pincette.jes.util.Validation.validator;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.Jq.MapModuleLoader.mapModuleLoader;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getArray;
import static net.pincette.json.JsonUtil.getObject;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getStrings;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.json.streams.Common.AGGREGATE;
import static net.pincette.json.streams.Common.AGGREGATE_TYPE;
import static net.pincette.json.streams.Common.BACKOFF;
import static net.pincette.json.streams.Common.COLLECTION_CHANGES;
import static net.pincette.json.streams.Common.COMMAND;
import static net.pincette.json.streams.Common.EVENT;
import static net.pincette.json.streams.Common.EVENT_FULL;
import static net.pincette.json.streams.Common.FROM_COLLECTION;
import static net.pincette.json.streams.Common.FROM_COLLECTIONS;
import static net.pincette.json.streams.Common.FROM_STREAM;
import static net.pincette.json.streams.Common.FROM_STREAMS;
import static net.pincette.json.streams.Common.FROM_TOPIC;
import static net.pincette.json.streams.Common.FROM_TOPICS;
import static net.pincette.json.streams.Common.JOIN;
import static net.pincette.json.streams.Common.JSLT_IMPORTS;
import static net.pincette.json.streams.Common.LAG;
import static net.pincette.json.streams.Common.LEFT;
import static net.pincette.json.streams.Common.LOG;
import static net.pincette.json.streams.Common.MERGE;
import static net.pincette.json.streams.Common.NAME;
import static net.pincette.json.streams.Common.ON;
import static net.pincette.json.streams.Common.PARTS;
import static net.pincette.json.streams.Common.PIPELINE;
import static net.pincette.json.streams.Common.PREPROCESSOR;
import static net.pincette.json.streams.Common.REDUCER;
import static net.pincette.json.streams.Common.REPLY;
import static net.pincette.json.streams.Common.RIGHT;
import static net.pincette.json.streams.Common.S3ATTACHMENTS;
import static net.pincette.json.streams.Common.S3CSV;
import static net.pincette.json.streams.Common.S3OUT;
import static net.pincette.json.streams.Common.S3TRANSFER;
import static net.pincette.json.streams.Common.SCRIPT_IMPORTS;
import static net.pincette.json.streams.Common.SIGN_JWT;
import static net.pincette.json.streams.Common.STREAM;
import static net.pincette.json.streams.Common.STREAM_TYPES;
import static net.pincette.json.streams.Common.TO_COLLECTION;
import static net.pincette.json.streams.Common.TO_TOPIC;
import static net.pincette.json.streams.Common.TYPE;
import static net.pincette.json.streams.Common.VALIDATE;
import static net.pincette.json.streams.Common.VALIDATOR;
import static net.pincette.json.streams.Common.VALIDATOR_IMPORTS;
import static net.pincette.json.streams.Common.addOtelLogger;
import static net.pincette.json.streams.Common.application;
import static net.pincette.json.streams.Common.applicationAttributes;
import static net.pincette.json.streams.Common.configValueApp;
import static net.pincette.json.streams.Common.findJson;
import static net.pincette.json.streams.Common.getCommands;
import static net.pincette.json.streams.Common.meterProvider;
import static net.pincette.json.streams.Common.namespace;
import static net.pincette.json.streams.Common.numberLines;
import static net.pincette.json.streams.Common.saveMessage;
import static net.pincette.json.streams.Common.tryToGetForever;
import static net.pincette.json.streams.Common.version;
import static net.pincette.json.streams.LagStage.lagStage;
import static net.pincette.json.streams.LogStage.logStage;
import static net.pincette.json.streams.Logging.LOGGER;
import static net.pincette.json.streams.Logging.exception;
import static net.pincette.json.streams.Logging.getLogger;
import static net.pincette.json.streams.Logging.severe;
import static net.pincette.json.streams.S3AttachmentsStage.s3AttachmentsStage;
import static net.pincette.json.streams.S3CsvStage.s3CsvStage;
import static net.pincette.json.streams.S3OutStage.s3OutStage;
import static net.pincette.json.streams.S3TransferStage.s3TransferStage;
import static net.pincette.json.streams.SignJwtStage.signJwtStage;
import static net.pincette.json.streams.TestExceptionStage.testExceptionStage;
import static net.pincette.json.streams.ValidateStage.validateStage;
import static net.pincette.mongo.BsonUtil.fromBson;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.BsonUtil.isoDateJson;
import static net.pincette.mongo.Collection.findOne;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.rs.Async.mapAsyncSequential;
import static net.pincette.rs.BackpressureTimout.backpressureTimeout;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Probe.probe;
import static net.pincette.rs.Probe.probeCancel;
import static net.pincette.rs.Probe.probeError;
import static net.pincette.rs.Probe.probeValue;
import static net.pincette.rs.Util.duplicateFilter;
import static net.pincette.rs.Util.onErrorProcessor;
import static net.pincette.rs.Util.pullForever;
import static net.pincette.rs.Util.retryPublisher;
import static net.pincette.rs.Util.tap;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Do.withValue;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.concat;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.rethrow;
import static net.pincette.util.Util.tryToDo;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToDoSilent;
import static net.pincette.util.Util.tryToGet;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;

import com.mongodb.MongoCommandException;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import io.opentelemetry.api.common.Attributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.jes.Aggregate;
import net.pincette.jes.JsonFields;
import net.pincette.jes.Reducer;
import net.pincette.jes.tel.EventTrace;
import net.pincette.json.Jq;
import net.pincette.json.Jslt;
import net.pincette.json.Jslt.MapResolver;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.Validator;
import net.pincette.mongo.Validator.Resolved;
import net.pincette.mongo.streams.Pipeline;
import net.pincette.mongo.streams.Stage;
import net.pincette.rs.Fanout;
import net.pincette.rs.Mapper;
import net.pincette.rs.Merge;
import net.pincette.rs.PassThrough;
import net.pincette.rs.Util;
import net.pincette.rs.streams.Message;
import net.pincette.rs.streams.Streams;
import net.pincette.util.Cases;
import net.pincette.util.Pair;
import net.pincette.util.State;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

class App<T, U, V, W> {
  private static final String AGGREGATE_SHARDS = "aggregateShards";
  private static final String BACKPRESSURE_TIMEOUT = "backpressureTimeout";
  private static final String COLLECTION_PREFIX = "collection-";
  private static final int DEFAULT_AGGREGATE_SHARDS = 10;
  private static final int DEFAULT_TRACE_SAMPLE_PERCENTAGE = 10;
  private static final String IN = "in";
  private static final String INVALID_COMMAND = "invalid-command";
  private static final String JOIN_TIMESTAMP = "_join_timestamp";
  private static final String MERGED = "merged";
  private static final String METRIC = "json_streams.messages";
  private static final String OUT = "out";
  private static final String PART = "part";
  private static final String REDUCER_STATE = "state";
  private static final String TEST_EXCEPTION = "$testException";
  private static final String TOKEN = "token";
  private static final String TO_STRING = "toString";
  private static final String TRACE = "trace";
  private static final String TRACE_ID = "traceId";
  private static final String TRACE_SAMPLE_PERCENTAGE = "traceSamplePercentage";
  private static final String TRACES_TOPIC = "tracesTopic";
  private static final String UNIQUE_EXPRESSION = "uniqueExpression";
  private static final String WINDOW = "window";

  private final int aggregateShards;
  private final Attributes attributes;
  private final Duration backpressureTimeout;
  private final Context context;
  private final Set<AutoCloseable> counters = new HashSet<>();
  private final EventTrace eventTrace;
  private final Function<String, Streams<String, JsonObject, T, U>> getBuilder;
  private final Function<String, Streams<String, String, V, W>> getStringBuilder;
  private final Supplier<CompletionStage<Map<String, Map<Partition, Long>>>> messageLag;
  private final Consumer<Throwable> onError;
  private final JsonObject specification;
  private final Map<String, SubscriptionMonitor<Message<String, JsonObject>>> streams =
      new HashMap<>();
  private Streams<String, JsonObject, T, U> builder;
  private Set<String> consumedStreams;
  private BiConsumer<Streams<String, JsonObject, T, U>, Streams<String, String, V, W>> onCreated;
  private Map<String, List<Subscriber<Message<String, JsonObject>>>> subscribers;
  private Streams<String, String, V, W> stringBuilder;
  private Thread thread;

  App() {
    this(null, null, null, null, null, null);
  }

  private App(
      final JsonObject specification,
      final Function<String, Streams<String, JsonObject, T, U>> getBuilder,
      final Function<String, Streams<String, String, V, W>> getStringBuilder,
      final Supplier<CompletionStage<Map<String, Map<Partition, Long>>>> messageLag,
      final Consumer<Throwable> onError,
      final Context context) {
    this.specification = specification;
    this.getBuilder = getBuilder;
    this.getStringBuilder = getStringBuilder;
    this.messageLag = messageLag;
    this.onError = onError;
    this.context = context;
    this.attributes =
        specification != null && context != null
            ? applicationAttributes(name(), context).build()
            : null;
    this.eventTrace =
        context != null && specification != null && shouldTrace(specification)
            ? new EventTrace()
                .withServiceNamespace(namespace(context.config))
                .withServiceName(name())
                .withServiceVersion(version(specification))
            : null;
    this.backpressureTimeout =
        context != null
            ? configValueApp(context.config::getDuration, BACKPRESSURE_TIMEOUT, name()).orElse(null)
            : null;
    this.aggregateShards =
        context != null
            ? configValueApp(context.config::getInt, AGGREGATE_SHARDS, name())
                .orElse(DEFAULT_AGGREGATE_SHARDS)
            : DEFAULT_AGGREGATE_SHARDS;
  }

  private static Optional<Pair<String, String>> aggregateTypeParts(final JsonObject specification) {
    final var type = specification.getString(AGGREGATE_TYPE);

    return Optional.of(type.indexOf('-'))
        .filter(index -> index != -1)
        .map(index -> pair(type.substring(0, index), type.substring(index + 1)));
  }

  private static Map<String, String> attributesToMap(final Attributes attributes) {
    return map(
        attributes.asMap().entrySet().stream()
            .map(e -> pair(e.getKey().getKey(), e.getValue().toString())));
  }

  private static String collectionKey(final String name) {
    return COLLECTION_PREFIX + name;
  }

  private static Stream<String> collectionNames(final JsonObject part) {
    return fromNames(part, FROM_COLLECTION, FROM_COLLECTIONS);
  }

  private static String command(final Message<String, JsonObject> message) {
    return message.value.getString(JsonFields.COMMAND, "unknown");
  }

  private static String commandTelemetryName(final Message<String, JsonObject> message) {
    return (hasError(message.value) ? INVALID_COMMAND : COMMAND) + "." + command(message);
  }

  private static Context completeContext(
      final Context context,
      final JsonObject specification,
      final Supplier<CompletionStage<Map<String, Map<Partition, Long>>>> messageLag) {
    final var application = application(specification);
    final var scriptImports =
        getScriptImports(specification)
            .map(App::convertScriptImports)
            .orElseGet(Collections::emptyMap);
    final var features =
        context
            .features
            .withJsltResolver(new MapResolver(scriptImports))
            .withJqModuleLoader(mapModuleLoader(scriptImports));
    final var logger = getLogger(application);
    final var withValidator =
        context
            .withLogger(() -> logger)
            .withFeatures(features)
            .withValidator(
                new Validator(
                    features,
                    (id, parent) ->
                        getObject(specification, "/" + VALIDATOR_IMPORTS + "/" + id)
                            .map(v -> new Resolved(v, id))));
    final var result =
        withValidator.withStageExtensions(stageExtensions(withValidator, messageLag));

    addOtelLogger(application, version(specification), result);

    return result;
  }

  private static Set<String> consumedStreams(final JsonArray parts) {
    return parts(parts, STREAM_TYPES).flatMap(App::streamNames).collect(toSet());
  }

  private static Map<String, String> convertScriptImports(final JsonObject imports) {
    return imports.entrySet().stream()
        .filter(e -> isString(e.getValue()))
        .collect(toMap(Entry::getKey, e -> asString(e.getValue()).getString()));
  }

  private static JsonObject createJoinMessage(final JsonObject json) {
    return createObjectBuilder(json).add(JOIN_TIMESTAMP, isoDateJson(now())).build();
  }

  private static Map<String, List<Subscriber<Message<String, JsonObject>>>> createSubscribers(
      final JsonArray parts) {
    return map(
        parts(parts, STREAM_TYPES)
            .flatMap(
                p ->
                    concat(
                        Stream.of(p.getString(NAME)), collectionNames(p).map(App::collectionKey)))
            .collect(toSet())
            .stream()
            .map(name -> pair(name, new ArrayList<>())));
  }

  private static String eventTelemetryName(final Message<String, JsonObject> message) {
    return EVENT + "." + command(message);
  }

  private static CompletionStage<Pair<Message<String, JsonObject>, Publisher<JsonObject>>>
      findOtherSideJoin(
          final Message<String, JsonObject> message,
          final MongoCollection<Document> otherCollection,
          final JsonValue otherCriterion,
          final JsonValue key,
          final int window) {
    return findJson(otherCollection, joinFilter(key, otherCriterion, window))
        .thenApply(results -> pair(message, results));
  }

  private static Stream<String> fromNames(
      final JsonObject part, final String single, final String plural) {
    return concat(
        ofNullable(part.getString(single, null)).stream(),
        getStrings(part, plural),
        getString(part, "/" + LEFT + "/" + single).stream(),
        getString(part, "/" + RIGHT + "/" + single).stream());
  }

  private static JsonArray getPipeline(final JsonObject specification, final String field) {
    return getArray(specification, "/" + field).orElseGet(JsonUtil::emptyArray);
  }

  private static Optional<JsonObject> getScriptImports(final JsonObject specification) {
    final JsonObject jslt = specification.getJsonObject(JSLT_IMPORTS); // For backward compatibility
    final JsonObject script = specification.getJsonObject(SCRIPT_IMPORTS);

    return tryWith(() -> jslt != null && script != null ? JsonUtil.merge(jslt, script) : null)
        .or(() -> jslt)
        .or(() -> script)
        .get();
  }

  private static Bson joinFilter(
      final JsonValue key, final JsonValue otherCriterion, final int window) {
    return fromJson(
        o(
            f(
                "$and",
                a(
                    o(f(JOIN_TIMESTAMP, o(f("$gte", isoDateJson(now().minus(ofMillis(window))))))),
                    o(f("$expr", o(f("$eq", a(otherCriterion, key)))))))));
  }

  private static JsonValue joinKeyExpression(final JsonObject specification, final String side) {
    return specification.getValue("/" + side + "/" + ON);
  }

  private static Message<String, JsonObject> joinedMessage(
      final JsonObject thisSideMessage,
      final JsonObject otherSideMessage,
      final ThisSide thisSide,
      final OtherSide otherSide) {
    final String id = toNative(thisSide.key.apply(thisSideMessage)).toString();

    return message(
        id,
        createObjectBuilder()
            .add(ID, id)
            .add(thisSide.name, removeJoinTimestamp(thisSideMessage))
            .add(otherSide.name, removeJoinTimestamp(otherSideMessage))
            .build());
  }

  private static Map<String, String> partLabel(final JsonObject part) {
    return map(pair(PART, part.getString(NAME)));
  }

  private static Stream<JsonObject> parts(final JsonArray parts, final Set<String> types) {
    return parts.stream()
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .filter(part -> types.contains(part.getString(TYPE)));
  }

  private static JsonObject removeJoinTimestamp(final JsonObject json) {
    return createObjectBuilder(json).remove(JOIN_TIMESTAMP).build();
  }

  private static boolean shouldTrace(final JsonObject specification) {
    return specification.getBoolean(TRACE, true);
  }

  private static Map<String, Stage> stageExtensions(
      final Context context,
      final Supplier<CompletionStage<Map<String, Map<Partition, Long>>>> messageLag) {
    return merge(
        ofNullable(context.stageExtensions).orElseGet(Collections::emptyMap),
        map(
            pair(LAG, lagStage(messageLag, context)),
            pair(LOG, logStage(context)),
            pair(SIGN_JWT, signJwtStage(context)),
            pair(S3ATTACHMENTS, s3AttachmentsStage(context)),
            pair(S3CSV, s3CsvStage(context)),
            pair(S3OUT, s3OutStage(context)),
            pair(S3TRANSFER, s3TransferStage(context)),
            pair(TEST_EXCEPTION, testExceptionStage()),
            pair(VALIDATE, validateStage(context))));
  }

  private static Stream<String> streamNames(final JsonObject part) {
    return fromNames(part, FROM_STREAM, FROM_STREAMS);
  }

  private static String stripScriptPrefix(final String script) {
    return Cases.<String, String>withValue(script)
        .or(s -> s.startsWith("jq:"), s -> s.substring("jq:".length()))
        .or(s -> s.startsWith("jslt:"), s -> s.substring("jslt:".length()))
        .get()
        .orElse(script);
  }

  private static Message<String, JsonObject> validationResult(
      final Message<String, JsonObject> message, final JsonObject result) {
    return hasError(result) ? message.withValue(result) : message;
  }

  private static <T> Consumer<T> wrapException(final Consumer<T> c) {
    return v ->
        tryToDo(() -> c.accept(v), e -> severe(() -> e.getMessage() + "\n" + getStackTrace(e)));
  }

  private static Optional<ChangeStreamDocument<Document>> youngest(
      final List<ChangeStreamDocument<Document>> changes) {
    return changes.stream()
        .filter(c -> c.getClusterTime() != null)
        .max(Comparator.comparing(c -> c.getClusterTime().getTime() + c.getClusterTime().getInc()));
  }

  private void addAggregateOutputTelemetry(
      final Aggregate<T, U> aggregate, final JsonObject specification) {
    final var partLabel = partLabel(specification);
    final var scope = scope(specification);

    telemetryProcessor(scope, App::eventTelemetryName, partLabel)
        .ifPresent(p -> pullForever(with(builder.from(aggregate.topic(EVENT))).map(p).get()));
    telemetryProcessor(scope, App::commandTelemetryName, partLabel)
        .ifPresent(
            p ->
                pullForever(
                    with(builder.from(aggregate.topic(REPLY)))
                        .filter(m -> hasError(m.value))
                        .map(p)
                        .get()));
  }

  private Aggregate<T, U> addPreprocessor(
      final Aggregate<T, U> aggregate, final JsonObject specification) {
    final Supplier<Processor<Message<String, JsonObject>, Message<String, JsonObject>>> pipeline =
        () -> createPipeline(specification, PREPROCESSOR);

    return telemetryProcessor(
            scope(specification), App::commandTelemetryName, partLabel(specification))
        .map(
            p ->
                aggregate.withCommandProcessor(
                    specification.containsKey(PREPROCESSOR) ? box(p, pipeline.get()) : p))
        .orElseGet(
            () ->
                specification.containsKey(PREPROCESSOR)
                    ? aggregate.withCommandProcessor(pipeline.get())
                    : aggregate);
  }

  private void addStream(final Aggregate<T, U> aggregate, final String type, final String purpose) {
    final String name = type + "-" + purpose;

    if (consumedStreams.contains(name)) {
      streams.put(name, new SubscriptionMonitor<>(builder.from(aggregate.topic(purpose))));
      subscribers.put(name, new ArrayList<>());
    }
  }

  private void addStreams(final Aggregate<T, U> aggregate) {
    final var type = aggregate.fullType();

    addStream(aggregate, type, AGGREGATE);
    addStream(aggregate, type, COMMAND);
    addStream(aggregate, type, EVENT);
    addStream(aggregate, type, EVENT_FULL);
    addStream(aggregate, type, REPLY);
  }

  private Publisher<Message<String, JsonObject>> addSubscriber(final String name) {
    final Processor<Message<String, JsonObject>, Message<String, JsonObject>> pass = passThrough();

    subscribers.get(name).add(pass);

    return pass;
  }

  private String backpressureName(final JsonObject specification) {
    return name() + ":" + specification.getString(NAME);
  }

  private void connectStreams() {
    streams.forEach(
        (k, v) ->
            ofNullable(subscribers.get(k))
                .ifPresent(
                    list -> {
                      if (list.size() == 1) {
                        v.subscribe(list.getFirst());
                      } else if (!list.isEmpty()) {
                        v.subscribe(Fanout.of(list));
                      }
                    }));
  }

  private void connectToCollection(
      final Publisher<Message<String, JsonObject>> stream, final String collection) {
    final MongoCollection<Document> col = context.database.getCollection(collection);

    pullForever(
        with(stream)
            .mapAsync(message -> saveMessage(col, message))
            .map(onErrorProcessor(onError::accept))
            .get());
  }

  private void connectToTopic(final String topic, final JsonObject specification) {
    final Publisher<Message<String, JsonObject>> source =
        addSubscriber(specification.getString(NAME));

    if (specification.getBoolean(TO_STRING, false)) {
      stringBuilder.to(topic, with(source).map(m -> message(m.key, string(m.value))).get());
    } else {
      builder.to(topic, source);
    }
  }

  private Aggregate<T, U> createAggregate(final String app, final String type) {
    return new Aggregate<T, U>()
        .withApp(app)
        .withType(type)
        .withBuilder(builder)
        .withMongoDatabase(context.database)
        .withMongoClient(context.client)
        .withShards(aggregateShards)
        .withLogger(logger());
  }

  private void createAggregate(final JsonObject specification) {
    aggregateTypeParts(specification)
        .ifPresent(
            aggregateType -> {
              final var aggregate =
                  reducers(
                      reducerProcessors(
                          preprocessors(
                              createAggregate(
                                  specification, aggregateType.first, aggregateType.second),
                              specification),
                          specification),
                      specification);

              aggregate.build();
              addStreams(aggregate);
              addAggregateOutputTelemetry(aggregate, specification);
            });
  }

  private Aggregate<T, U> createAggregate(
      final JsonObject specification, final String app, final String type) {
    return create(() -> createAggregate(app, type))
        .updateIf(() -> ofNullable(context.environment), Aggregate::withEnvironment)
        .updateIf(
            () -> getValue(specification, "/" + UNIQUE_EXPRESSION), Aggregate::withUniqueExpression)
        .update(a -> addPreprocessor(a, specification))
        .build();
  }

  private void createApplication() {
    createParts(specification.getJsonArray(PARTS));
  }

  private Publisher<Message<String, JsonObject>> createCollectionStream(final String collection) {
    final State<Boolean> failed = new State<>(false);
    final State<Boolean> testCollectionLoaded = new State<>(false);
    final Supplier<ChangeStreamPublisher<Document>> watch =
        () -> context.database.getCollection(collection).watch(COLLECTION_CHANGES);

    return retryPublisher(
        () ->
            with(toFlowPublisher(
                    TRUE.equals(failed.get())
                        ? watch.get()
                        : startAfter(watch.get(), collection).fullDocument(UPDATE_LOOKUP)))
                .commit(changes -> saveResumeTokens(changes, collection))
                .map(Common::changedDocument)
                .map(Common::toMessage)
                .map(
                    probe(
                        n -> {
                          if (context.testLoadCollection != null && !testCollectionLoaded.get()) {
                            testCollectionLoaded.set(true);
                            context.testLoadCollection.accept(collection);
                          }
                        }))
                .get(),
        BACKOFF,
        e -> {
          exception(e);

          if (e instanceof MongoCommandException) {
            // The command history was no longer in the oplog.
            failed.set(true);
          }
        });
  }

  private Publisher<Message<String, JsonObject>> createJoin(final JsonObject specification) {
    final var leftCollection = context.database.getCollection(joinCollection(specification, LEFT));
    final var leftKeyExpression = joinKeyExpression(specification, LEFT);
    final var leftKey = joinKey(leftKeyExpression);
    final var rightCollection =
        context.database.getCollection(joinCollection(specification, RIGHT));
    final var rightKeyExpression = joinKeyExpression(specification, RIGHT);
    final var rightKey = joinKey(rightKeyExpression);
    final var window = specification.getInt(WINDOW, MAX_VALUE);

    return with(Merge.of(
            joinStream(
                specification,
                new ThisSide(LEFT, leftCollection, leftKey),
                new OtherSide(RIGHT, rightCollection, rightKeyExpression),
                window),
            joinStream(
                specification,
                new ThisSide(RIGHT, rightCollection, rightKey),
                new OtherSide(LEFT, leftCollection, leftKeyExpression),
                window)))
        .buffer(2) // Make sure the two branches get a request, otherwise it won't start.
        .map(duplicateFilter(m -> m.value, ofSeconds(1)))
        .get(); // When matching messages arrive at the same time, there can be duplicates.
  }

  private Publisher<Message<String, JsonObject>> createMerge(final JsonObject specification) {
    return with(Cases.<JsonObject, Stream<Publisher<Message<String, JsonObject>>>>withValue(
                specification)
            .or(s -> s.containsKey(FROM_TOPICS), s -> getStrings(s, FROM_TOPICS).map(builder::from))
            .or(
                s -> s.containsKey(FROM_STREAMS),
                s -> getStrings(s, FROM_STREAMS).map(this::addSubscriber))
            .or(
                s -> s.containsKey(FROM_COLLECTIONS),
                s ->
                    getStrings(s, FROM_COLLECTIONS)
                        .map(App::collectionKey)
                        .map(this::addSubscriber))
            .get()
            .map(
                stream ->
                    stream
                        .map(
                            p ->
                                with(p)
                                    .backpressureTimeout(
                                        backpressureTimeout, () -> backpressureName(specification))
                                    .get())
                        .toList())
            .map(Merge::of)
            .orElseGet(Util::empty))
        .map(
            telemetryProcessor(scope(specification), m -> MERGED, partLabel(specification))
                .orElseGet(PassThrough::passThrough))
        .get();
  }

  private Publisher<Message<String, JsonObject>> createPart(final JsonObject specification) {
    return switch (specification.getString(TYPE)) {
      case JOIN -> createJoin(specification);
      case MERGE -> createMerge(specification);
      case STREAM -> createStream(specification);
      default -> null;
    };
  }

  private void createParts(final JsonArray parts) {
    subscribers = createSubscribers(parts);
    consumedStreams = consumedStreams(parts);
    parts(parts, set(AGGREGATE)).forEach(this::createAggregate);
    parts(parts, STREAM_TYPES)
        .forEach(
            part ->
                streams.put(
                    part.getString(NAME),
                    toStream(new SubscriptionMonitor<>(createPart(part)), part)));
    parts(parts, STREAM_TYPES)
        .flatMap(App::collectionNames)
        .forEach(
            name ->
                streams.put(
                    collectionKey(name), new SubscriptionMonitor<>(createCollectionStream(name))));
    connectStreams();
    terminateOpenStreams();
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> createPipeline(
      final JsonObject specification, final String field) {
    return Pipeline.create(
        getPipeline(specification, field),
        new net.pincette.mongo.streams.Context()
            .withApp(name())
            .withDatabase(context.database)
            .withFeatures(context.features)
            .withStageExtensions(context.stageExtensions)
            .withProducer(context.producer::sendJson)
            .withLogger(logger())
            .withTrace(FINEST.equals(logger().getLevel())));
  }

  private Reducer createReducer(final String script, final JsonObject validator) {
    final var reducer =
        reducer(
            script.startsWith("jq:")
                ? transformerJq(stripScriptPrefix(script))
                : transformerJslt(stripScriptPrefix(script)));

    return withResolver(
        validator != null ? compose(validatorReducer(validator), reducer) : reducer,
        context.environment,
        context.database);
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>>
      createReducerProcessor(final JsonObject command, final JsonObject validator) {
    return pipe(resolveProcessor())
        .then(validatorProcessor(validator))
        .then(createPipeline(command, REDUCER))
        .then(unresolveProcessor());
  }

  private Publisher<Message<String, JsonObject>> createStream(final JsonObject specification) {
    return fromStream(
        ofNullable(createPipeline(specification, PIPELINE))
            .map(
                p ->
                    wrapTelemetry(
                        box(
                            backpressureTimeout(
                                backpressureTimeout, () -> backpressureName(specification)),
                            p),
                        specification))
            .orElseGet(PassThrough::passThrough),
        specification);
  }

  private String envSuffix() {
    return ofNullable(context.environment).map(e -> "-" + e).orElse("");
  }

  private Publisher<Message<String, JsonObject>> fromStream(
      final Processor<Message<String, JsonObject>, Message<String, JsonObject>> stream,
      final JsonObject specification) {
    withValue(specification)
        .orGet(
            s -> ofNullable(s.getString(FROM_TOPIC, null)).map(topic -> builder.from(topic)),
            publisher -> publisher.subscribe(stream))
        .orGet(
            s -> ofNullable(s.getString(FROM_STREAM, null)),
            str -> subscribers.get(str).add(stream))
        .orGet(
            s -> ofNullable(s.getString(FROM_COLLECTION, null)).map(App::collectionKey),
            col -> subscribers.get(col).add(stream));

    return stream;
  }

  private String joinCollection(final JsonObject specification, final String side) {
    return name() + "-" + specification.getString(NAME) + "-join-" + side + envSuffix();
  }

  private Function<JsonObject, JsonValue> joinKey(final JsonValue expression) {
    return function(expression, context.features);
  }

  private Publisher<Message<String, JsonObject>> joinSource(final JsonObject specification) {
    return Cases.<JsonObject, Publisher<Message<String, JsonObject>>>withValue(specification)
        .orGet(s -> ofNullable(s.getString(FROM_TOPIC, null)), builder::from)
        .orGet(s -> ofNullable(s.getString(FROM_STREAM, null)), this::addSubscriber)
        .orGet(
            s -> ofNullable(s.getString(FROM_COLLECTION, null)).map(App::collectionKey),
            this::addSubscriber)
        .get()
        .orElseGet(Util::empty);
  }

  private Publisher<Message<String, JsonObject>> joinStream(
      final JsonObject specification,
      final ThisSide thisSide,
      final OtherSide otherSide,
      final int window) {
    final var partLabel = partLabel(specification);
    final var scope = scope(specification);

    return with(joinSource(specification.getJsonObject(thisSide.name)))
        .backpressureTimeout(
            backpressureTimeout,
            () -> name() + ":" + specification.getString(NAME) + ":" + thisSide.name)
        .map(
            probeCancel(
                () -> severe(() -> "Part " + string(specification, false) + " was cancelled")))
        .map(
            telemetryProcessor(scope, m -> thisSide.name + "." + IN, partLabel)
                .orElseGet(PassThrough::passThrough))
        .map(message -> message.withValue(createJoinMessage(message.value)))
        .mapAsync(message -> saveMessage(thisSide.collection, message))
        .mapAsync(
            message ->
                findOtherSideJoin(
                    message,
                    otherSide.collection,
                    otherSide.criterion,
                    thisSide.key.apply(message.value),
                    window))
        .flatMap(
            pair ->
                with(pair.second)
                    .map(found -> joinedMessage(pair.first.value, found, thisSide, otherSide))
                    .get())
        .map(
            telemetryProcessor(scope, m -> thisSide.name + "." + OUT, partLabel)
                .orElseGet(PassThrough::passThrough))
        .map(
            probeError(
                e ->
                    severe(
                        () ->
                            "Part "
                                + string(specification, false)
                                + " had error "
                                + e.getMessage())))
        .get();
  }

  private Logger logger() {
    return getLogger(name());
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> mapToEventTrace(
      final String scope,
      final Function<Message<String, JsonObject>, String> name,
      final Map<String, String> labels) {
    final var percentage =
        configValue(context.config::getInt, TRACE_SAMPLE_PERCENTAGE)
            .orElse(DEFAULT_TRACE_SAMPLE_PERCENTAGE);

    return box(
        map(
            message ->
                traceMessage(
                        message.value,
                        eventTrace,
                        scope + "." + name.apply(message),
                        labels,
                        percentage)
                    .map(m -> message.withValue(m).withKey(m.getString(TRACE_ID)))
                    .orElse(null)),
        filter(Objects::nonNull));
  }

  private Attributes metricLabels(final String name, final Map<String, String> labels) {
    return addLabels(
        attributes,
        merge(
            labels,
            map(pair(PART, ofNullable(labels.get(PART)).map(p -> p + ".").orElse("") + name))));
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> metricsProcessor(
      final String scope,
      final Function<Message<String, JsonObject>, String> name,
      final Map<String, String> labels) {
    return meterProvider(context)
        .map(p -> p.meterBuilder(scope).build())
        .map(
            m ->
                probeValue(
                    wrapException(
                        counter(
                            m,
                            METRIC,
                            (Message<String, JsonObject> message) ->
                                metricLabels(name.apply(message), labels),
                            message -> 1L,
                            counters))))
        .orElse(null);
  }

  String name() {
    return application(specification);
  }

  void onCreated(
      final BiConsumer<Streams<String, JsonObject, T, U>, Streams<String, String, V, W>> consumer) {
    this.onCreated = consumer;
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> preprocessor(
      final JsonObject specification,
      final Supplier<Processor<Message<String, JsonObject>, Message<String, JsonObject>>>
          pipeline) {
    return telemetryProcessor(
            scope(specification), App::commandTelemetryName, partLabel(specification))
        .map(p -> box(p, pipeline.get()))
        .orElseGet(pipeline);
  }

  private Aggregate<T, U> preprocessors(
      final Aggregate<T, U> aggregate, final JsonObject specification) {
    return getCommands(specification)
        .filter(pair -> pair.second.containsKey(PREPROCESSOR))
        .reduce(
            aggregate,
            (a, p) ->
                a.withCommandProcessor(
                    p.first,
                    () ->
                        preprocessor(specification, () -> createPipeline(p.second, PREPROCESSOR))),
            (a1, a2) -> a1);
  }

  private Aggregate<T, U> reducerProcessors(
      final Aggregate<T, U> aggregate, final JsonObject specification) {
    return getCommands(specification)
        .filter(pair -> isArray(pair.second.get(REDUCER)))
        .reduce(
            aggregate,
            (a, p) ->
                a.withReducer(
                    p.first,
                    () ->
                        box(
                            backpressureTimeout(
                                backpressureTimeout,
                                () -> name() + ":" + aggregate.fullType() + ":" + p.first),
                            createReducerProcessor(p.second, p.second.getJsonObject(VALIDATOR)))),
            (a1, a2) -> a1);
  }

  private Aggregate<T, U> reducers(
      final Aggregate<T, U> aggregate, final JsonObject specification) {
    return getCommands(specification)
        .filter(pair -> isString(pair.second.get(REDUCER)))
        .reduce(
            aggregate,
            (a, p) ->
                a.withReducer(
                    p.first,
                    createReducer(p.second.getString(REDUCER), p.second.getJsonObject(VALIDATOR))),
            (a1, a2) -> a1);
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> resolveProcessor() {
    return mapAsyncSequential(
        m ->
            resolve(
                    list(m.value.getJsonObject(COMMAND), m.value.getJsonObject(REDUCER_STATE)),
                    context.environment,
                    context.database)
                .thenApply(
                    list ->
                        m.withValue(
                            createObjectBuilder(m.value)
                                .add(COMMAND, list.getFirst())
                                .add(REDUCER_STATE, list.get(1))
                                .build())));
  }

  private String resumeTokenCollection() {
    return "resumetoken" + envSuffix();
  }

  private String resumeTokenKey(final String collection) {
    return name() + "-" + collection;
  }

  private CompletionStage<Boolean> saveResumeToken(
      final BsonDocument resumeToken, final String collection) {
    final String key = resumeTokenKey(collection);

    return tryToGetForever(
        () ->
            update(
                    context.database.getCollection(resumeTokenCollection()),
                    o(f(ID, v(key)), f(TOKEN, fromBson(resumeToken))))
                .thenApply(result -> must(result, r -> r)));
  }

  private CompletionStage<Boolean> saveResumeTokens(
      final List<ChangeStreamDocument<Document>> changes, final String collection) {
    return youngest(changes)
        .map(change -> saveResumeToken(change.getResumeToken(), collection))
        .orElseGet(() -> completedFuture(false));
  }

  private String scope(final JsonObject part) {
    return part.getString(NAME);
  }

  void start() {
    final String application = name();
    final String version = version(specification);

    LOGGER.log(INFO, "Starting {0} {1}", new Object[] {application, version});

    builder = getBuilder.apply(application).onError(onError);
    stringBuilder = getStringBuilder.apply(application).onError(onError);
    createApplication();

    if (onCreated != null) {
      onCreated.accept(builder, stringBuilder);
    }

    stringBuilder.start(); // String builders don't have a topic source.
    thread = new Thread(builder::start, application);
    thread.start();
  }

  private ChangeStreamPublisher<Document> startAfter(
      final ChangeStreamPublisher<Document> changes, final String collection) {
    return findOne(
            context.database.getCollection(resumeTokenCollection()),
            eq(ID, resumeTokenKey(collection)),
            BsonDocument.class,
            null)
        .thenApply(
            result ->
                result.map(doc -> doc.getDocument(TOKEN)).map(changes::startAfter).orElse(changes))
        .toCompletableFuture()
        .join();
  }

  void stop() {
    LOGGER.log(INFO, "Stopping {0} {1}", new Object[] {name(), version(specification)});
    counters.forEach(c -> tryToDoSilent(c::close));

    if (builder != null) {
      builder.stop();
    }

    if (thread != null) {
      tryToDoRethrow(thread::join);
    }

    LOGGER.log(INFO, "Stopped {0}", new Object[] {name()});
  }

  private Optional<Processor<Message<String, JsonObject>, Message<String, JsonObject>>>
      telemetryProcessor(
          final String scope,
          final Function<Message<String, JsonObject>, String> name,
          final Map<String, String> labels) {
    return Optional.of(
            pair(metricsProcessor(scope, name, labels), tracesProcessor(scope, name, labels)))
        .filter(p -> p.first != null || p.second != null)
        .map(
            p ->
                box(
                    p.first != null ? p.first : passThrough(),
                    p.second != null ? p.second : passThrough()));
  }

  private void terminateOpenStreams() {
    streams.values().stream()
        .filter(v -> v.subscriber == null)
        .forEach(p -> pullForever(with(p).map(onErrorProcessor(onError::accept)).get()));
  }

  private SubscriptionMonitor<Message<String, JsonObject>> toStream(
      final SubscriptionMonitor<Message<String, JsonObject>> stream,
      final JsonObject specification) {
    if (specification.containsKey(TO_TOPIC)) {
      connectToTopic(specification.getString(TO_TOPIC), specification);
    } else if (specification.containsKey(TO_COLLECTION)) {
      connectToCollection(stream, specification.getString(TO_COLLECTION));
    }

    return stream;
  }

  private Optional<JsonObject> traceMessage(
      final JsonObject json,
      final EventTrace eventTrace,
      final String name,
      final Map<String, String> labels,
      final int percentage) {
    return getString(json, "/" + CORR)
        .filter(corr -> retainTraceSample(corr, percentage))
        .map(
            corr ->
                eventTrace
                    .withTraceId(corr)
                    .withTimestamp(now())
                    .withName(name)
                    .withAttributes(attributesToMap(addLabels(attributes, labels)))
                    .withUsername(getUsername(json).orElse(""))
                    .withPayload(hasError(json) ? json : null)
                    .toJson()
                    .build());
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> tracesProcessor(
      final String scope,
      final Function<Message<String, JsonObject>, String> name,
      final Map<String, String> labels) {
    return ofNullable(eventTrace)
        .flatMap(e -> configValue(context.config::getString, TRACES_TOPIC))
        .map(
            topic -> {
              final var p = mapToEventTrace(scope, name, labels);

              builder.to(topic, p);
              return tap(p);
            })
        .orElse(null);
  }

  private UnaryOperator<JsonObject> transformerJq(final String jq) {
    return transformerScript(
        jq,
        tryToGet(
                () ->
                    Jq.transformerObject(
                        new Jq.Context(Jq.tryReader(jq))
                            .withModuleLoader(context.features.jqModuleLoader)),
                e -> {
                  exception(e, () -> jq, this::logger);
                  rethrow(e);
                  return null;
                })
            .orElse(j -> j));
  }

  private UnaryOperator<JsonObject> transformerJslt(final String jslt) {
    return transformerScript(
        jslt,
        tryToGet(
                () ->
                    Jslt.transformerObject(
                        new Jslt.Context(Jslt.tryReader(jslt))
                            .withResolver(context.features.jsltResolver)
                            .withFunctions(context.features.customJsltFunctions)),
                e -> {
                  exception(e, () -> jslt, this::logger);
                  rethrow(e);
                  return null;
                })
            .orElse(j -> j));
  }

  private UnaryOperator<JsonObject> transformerScript(
      final String script, final UnaryOperator<JsonObject> op) {
    return json ->
        tryToGet(
                () -> op.apply(json),
                e -> {
                  exception(
                      e,
                      () ->
                          "Script:\n"
                              + numberLines(script)
                              + "\n\nWith JSON:\n"
                              + string(json, true),
                      this::logger);
                  rethrow(e);
                  return null;
                })
            .orElse(null);
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> unresolveProcessor() {
    return Mapper.map(m -> m.withValue(unresolve(m.value)));
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> validatorProcessor(
      final JsonObject validator) {
    final var validatorFunction = validator != null ? validatorReducer(validator) : null;

    return validatorFunction != null
        ? mapAsyncSequential(
            m ->
                validatorFunction
                    .apply(m.value.getJsonObject(COMMAND), m.value.getJsonObject(REDUCER_STATE))
                    .thenApply(result -> validationResult(m, result)))
        : passThrough();
  }

  private Reducer validatorReducer(final JsonObject validator) {
    return validator(context.validator.validator(validator));
  }

  App<T, U, V, W> withBuilder(final Function<String, Streams<String, JsonObject, T, U>> builder) {
    return new App<>(specification, builder, getStringBuilder, messageLag, onError, context);
  }

  App<T, U, V, W> withContext(final Context context) {
    return new App<>(
        specification,
        getBuilder,
        getStringBuilder,
        messageLag,
        onError,
        specification != null && messageLag != null
            ? completeContext(context, specification, messageLag)
            : context);
  }

  App<T, U, V, W> withMessageLag(
      final Supplier<CompletionStage<Map<String, Map<Partition, Long>>>> messageLag) {
    return new App<>(
        specification,
        getBuilder,
        getStringBuilder,
        messageLag,
        onError,
        context != null && specification != null
            ? completeContext(context, specification, messageLag)
            : null);
  }

  App<T, U, V, W> withOnError(final Consumer<Throwable> onError) {
    return new App<>(specification, getBuilder, getStringBuilder, messageLag, onError, context);
  }

  App<T, U, V, W> withSpecification(final JsonObject specification) {
    return new App<>(
        specification,
        getBuilder,
        getStringBuilder,
        messageLag,
        onError,
        context != null && messageLag != null
            ? completeContext(context, specification, messageLag)
            : null);
  }

  App<T, U, V, W> withStringBuilder(
      final Function<String, Streams<String, String, V, W>> stringBuilder) {
    return new App<>(specification, getBuilder, stringBuilder, messageLag, onError, context);
  }

  private Processor<Message<String, JsonObject>, Message<String, JsonObject>> wrapTelemetry(
      final Processor<Message<String, JsonObject>, Message<String, JsonObject>> processor,
      final JsonObject specification) {
    final var partLabel = partLabel(specification);
    final var scope = scope(specification);

    return telemetryProcessor(scope, m -> IN, partLabel)
        .map(
            in ->
                (Processor<Message<String, JsonObject>, Message<String, JsonObject>>)
                    pipe(in)
                        .then(processor)
                        .then(
                            telemetryProcessor(scope, m -> OUT, partLabel)
                                .orElseGet(PassThrough::passThrough)))
        .orElse(processor);
  }

  private record OtherSide(
      String name, MongoCollection<Document> collection, JsonValue criterion) {}

  private static class SubscriptionMonitor<T> implements Publisher<T> {
    private final Publisher<T> publisher;
    private Subscriber<? super T> subscriber;

    private SubscriptionMonitor(final Publisher<T> publisher) {
      this.publisher = publisher;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
      publisher.subscribe(subscriber);
      this.subscriber = subscriber;
    }
  }

  private record ThisSide(
      String name, MongoCollection<Document> collection, Function<JsonObject, JsonValue> key) {}
}
