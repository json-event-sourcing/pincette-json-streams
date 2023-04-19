package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP;
import static java.lang.Boolean.TRUE;
import static java.lang.Integer.MAX_VALUE;
import static java.time.Duration.ofMinutes;
import static java.time.Instant.now;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static net.pincette.jes.Aggregate.reducer;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.Util.compose;
import static net.pincette.jes.util.Mongo.resolve;
import static net.pincette.jes.util.Mongo.unresolve;
import static net.pincette.jes.util.Mongo.withResolver;
import static net.pincette.jes.util.Validation.validator;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.Jslt.transformerObject;
import static net.pincette.json.Jslt.tryReader;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createObjectBuilder;
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
import static net.pincette.json.streams.Common.SIGN_JWT;
import static net.pincette.json.streams.Common.STREAM;
import static net.pincette.json.streams.Common.STREAM_TYPES;
import static net.pincette.json.streams.Common.TO_COLLECTION;
import static net.pincette.json.streams.Common.TO_TOPIC;
import static net.pincette.json.streams.Common.TYPE;
import static net.pincette.json.streams.Common.VALIDATE;
import static net.pincette.json.streams.Common.VALIDATOR;
import static net.pincette.json.streams.Common.VALIDATOR_IMPORTS;
import static net.pincette.json.streams.Common.addKafkaLogger;
import static net.pincette.json.streams.Common.application;
import static net.pincette.json.streams.Common.fatal;
import static net.pincette.json.streams.Common.findJson;
import static net.pincette.json.streams.Common.getCommands;
import static net.pincette.json.streams.Common.numberLines;
import static net.pincette.json.streams.Common.saveMessage;
import static net.pincette.json.streams.Common.tryToGetForever;
import static net.pincette.json.streams.Common.version;
import static net.pincette.json.streams.LagStage.lagStage;
import static net.pincette.json.streams.LogStage.logStage;
import static net.pincette.json.streams.Logging.LOGGER;
import static net.pincette.json.streams.Logging.exception;
import static net.pincette.json.streams.Logging.getLogger;
import static net.pincette.json.streams.S3AttachmentsStage.s3AttachmentsStage;
import static net.pincette.json.streams.S3CsvStage.s3CsvStage;
import static net.pincette.json.streams.S3OutStage.s3OutStage;
import static net.pincette.json.streams.SignJwtStage.signJwtStage;
import static net.pincette.json.streams.ValidateStage.validateStage;
import static net.pincette.mongo.BsonUtil.fromBson;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Collection.findOne;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.rs.Async.mapAsyncSequential;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Probe.probe;
import static net.pincette.rs.Util.devNull;
import static net.pincette.rs.Util.duplicateFilter;
import static net.pincette.rs.Util.retryPublisher;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Do.withValue;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.concat;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoRethrow;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;

import com.mongodb.MongoCommandException;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
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
import net.pincette.jes.Reducer;
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
  private static final String COLLECTION_PREFIX = "collection-";
  private static final String JOIN_TIMESTAMP = "_join_timestamp";
  private static final String REDUCER_STATE = "state";
  private static final String TOKEN = "token";
  private static final String TO_STRING = "toString";
  private static final String UNIQUE_EXPRESSION = "uniqueExpression";
  private static final String WINDOW = "window";

  private final Context context;
  private final Function<String, Streams<String, JsonObject, T, U>> getBuilder;
  private final Function<String, Streams<String, String, V, W>> getStringBuilder;
  private final Supplier<CompletionStage<Map<String, Map<Partition, Long>>>> messageLag;
  private final OnError onError;
  private final JsonObject specification;
  private final Map<String, Publisher<Message<String, JsonObject>>> streams = new HashMap<>();
  private Streams<String, JsonObject, T, U> builder;
  private Set<String> consumedStreams;
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
      final OnError onError,
      final Context context) {
    this.specification = specification;
    this.getBuilder = getBuilder;
    this.getStringBuilder = getStringBuilder;
    this.messageLag = messageLag;
    this.onError = onError;
    this.context = context;
  }

  private static Optional<Pair<String, String>> aggregateTypeParts(final JsonObject specification) {
    final var type = specification.getString(AGGREGATE_TYPE);

    return Optional.of(type.indexOf('-'))
        .filter(index -> index != -1)
        .map(index -> pair(type.substring(0, index), type.substring(index + 1)));
  }

  private static String collectionKey(final String name) {
    return COLLECTION_PREFIX + name;
  }

  private static Stream<String> collectionNames(final JsonObject part) {
    return fromNames(part, FROM_COLLECTION, FROM_COLLECTIONS);
  }

  private static Context completeContext(
      final Context context,
      final JsonObject specification,
      final Supplier<CompletionStage<Map<String, Map<Partition, Long>>>> messageLag) {
    final var application = application(specification);
    final var features =
        context.features.withJsltResolver(
            new MapResolver(
                ofNullable(specification.getJsonObject(JSLT_IMPORTS))
                    .map(App::convertJsltImports)
                    .orElseGet(Collections::emptyMap)));
    final var logger = getLogger(application);
    final var withValidator =
        context
            .withFeatures(features)
            .withValidator(
                new Validator(
                    features,
                    (id, parent) ->
                        getObject(specification, "/" + VALIDATOR_IMPORTS + "/" + id)
                            .map(v -> new Resolved(v, id))));

    final var result =
        withValidator
            .withStageExtensions(stageExtensions(withValidator, messageLag))
            .withLogger(() -> logger);

    addKafkaLogger(application, version(specification), result);

    return result;
  }

  private static Set<String> consumedStreams(final JsonArray parts) {
    return parts(parts, STREAM_TYPES).flatMap(App::streamNames).collect(toSet());
  }

  private static Map<String, String> convertJsltImports(final JsonObject jsltImports) {
    return jsltImports.entrySet().stream()
        .filter(e -> isString(e.getValue()))
        .collect(toMap(Entry::getKey, e -> asString(e.getValue()).getString()));
  }

  private static JsonObject createJoinMessage(final JsonObject json) {
    return createObjectBuilder(json).add(JOIN_TIMESTAMP, now().toEpochMilli()).build();
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
    return getValue(specification, "/" + field)
        .map(JsonValue::asJsonArray)
        .orElseGet(JsonUtil::emptyArray);
  }

  private static Bson joinFilter(
      final JsonValue key, final JsonValue otherCriterion, final int window) {
    return fromJson(
        o(
            f(
                "$and",
                a(
                    o(f(JOIN_TIMESTAMP, o(f("$gte", v(now().toEpochMilli() - window))))),
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

  private static Stream<JsonObject> parts(final JsonArray parts, final Set<String> types) {
    return parts.stream()
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .filter(part -> types.contains(part.getString(TYPE)));
  }

  private static void pullForever(final Publisher<Message<String, JsonObject>> publisher) {
    publisher.subscribe(devNull());
  }

  private static JsonObject removeJoinTimestamp(final JsonObject json) {
    return createObjectBuilder(json).remove(JOIN_TIMESTAMP).build();
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
            pair(VALIDATE, validateStage(context))));
  }

  private static Stream<String> streamNames(final JsonObject part) {
    return fromNames(part, FROM_STREAM, FROM_STREAMS);
  }

  private static Optional<ChangeStreamDocument<Document>> youngest(
      final List<ChangeStreamDocument<Document>> changes) {
    return changes.stream()
        .filter(c -> c.getClusterTime() != null)
        .max(Comparator.comparing(c -> c.getClusterTime().getTime() + c.getClusterTime().getInc()));
  }

  private void addStream(final Aggregate<T, U> aggregate, final String type, final String purpose) {
    final String name = type + "-" + purpose;

    if (consumedStreams.contains(name)) {
      streams.put(name, builder.from(aggregate.topic(purpose)));
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
    final Processor<Message<String, JsonObject>, Message<String, JsonObject>> probe =
        probe(n -> {}, v -> {}, () -> {}, onError::setError);

    subscribers.get(name).add(probe);

    return probe;
  }

  private void connectStreams() {
    streams.forEach(
        (k, v) ->
            ofNullable(subscribers.get(k))
                .ifPresent(
                    list -> {
                      if (list.size() == 1) {
                        v.subscribe(list.get(0));
                      } else if (!list.isEmpty()) {
                        v.subscribe(Fanout.of(list));
                      }
                    }));
  }

  private void connectToCollection(
      final Publisher<Message<String, JsonObject>> stream, final String collection) {
    final MongoCollection<Document> col = context.database.getCollection(collection);

    pullForever(with(stream).mapAsync(message -> saveMessage(col, message)).get());
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
                              create(
                                      () ->
                                          createAggregate(
                                              aggregateType.first, aggregateType.second))
                                  .updateIf(
                                      () -> ofNullable(context.environment),
                                      Aggregate::withEnvironment)
                                  .updateIf(
                                      () -> getValue(specification, "/" + UNIQUE_EXPRESSION),
                                      Aggregate::withUniqueExpression)
                                  .updateIf(
                                      a -> specification.containsKey(PREPROCESSOR),
                                      a ->
                                          a.withCommandProcessor(
                                              createPipeline(specification, PREPROCESSOR)))
                                  .build(),
                              specification),
                          specification),
                      specification);

              aggregate.build();
              addStreams(aggregate);
            });
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
        .map(duplicateFilter(m -> m.value, ofMinutes(1)))
        .get(); // When matching messages arrive at the same time there can be duplicates.
  }

  private Publisher<Message<String, JsonObject>> createMerge(final JsonObject specification) {
    return Cases.<JsonObject, Stream<Publisher<Message<String, JsonObject>>>>withValue(
            specification)
        .or(s -> s.containsKey(FROM_TOPICS), s -> getStrings(s, FROM_TOPICS).map(builder::from))
        .or(
            s -> s.containsKey(FROM_STREAMS),
            s -> getStrings(s, FROM_STREAMS).map(this::addSubscriber))
        .or(
            s -> s.containsKey(FROM_COLLECTIONS),
            s -> getStrings(s, FROM_COLLECTIONS).map(App::collectionKey).map(this::addSubscriber))
        .get()
        .map(stream -> stream.collect(toList()))
        .map(Merge::of)
        .orElseGet(Util::empty);
  }

  private Publisher<Message<String, JsonObject>> createPart(final JsonObject specification) {
    switch (specification.getString(TYPE)) {
      case JOIN:
        return createJoin(specification);
      case MERGE:
        return createMerge(specification);
      case STREAM:
        return createStream(specification);
      default:
        return null;
    }
  }

  private void createParts(final JsonArray parts) {
    subscribers = createSubscribers(parts);
    consumedStreams = consumedStreams(parts);
    parts(parts, set(AGGREGATE)).forEach(this::createAggregate);
    parts(parts, STREAM_TYPES)
        .forEach(part -> streams.put(part.getString(NAME), toStream(createPart(part), part)));
    parts(parts, STREAM_TYPES)
        .flatMap(App::collectionNames)
        .forEach(name -> streams.put(collectionKey(name), createCollectionStream(name)));
    connectStreams();
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

  private Reducer createReducer(final String jslt, final JsonObject validator) {
    final var reducer = reducer(transformer(jslt));

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
        ofNullable(createPipeline(specification, PIPELINE)).orElseGet(PassThrough::passThrough),
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
    return with(joinSource(specification.getJsonObject(thisSide.name)))
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
        .get();
  }

  private Logger logger() {
    return getLogger(name());
  }

  String name() {
    return application(specification);
  }

  private Aggregate<T, U> preprocessors(
      final Aggregate<T, U> aggregate, final JsonObject specification) {
    return getCommands(specification)
        .filter(pair -> pair.second.containsKey(PREPROCESSOR))
        .reduce(
            aggregate,
            (a, p) -> a.withCommandProcessor(p.first, createPipeline(p.second, PREPROCESSOR)),
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
                    p.first, createReducerProcessor(p.second, p.second.getJsonObject(VALIDATOR))),
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
                                .add(COMMAND, list.get(0))
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

  void start() {
    final String application = name();
    final String version = version(specification);

    LOGGER.log(INFO, "Starting {0} {1}", new Object[] {application, version});

    if (onError != null) {
      onError.clear();
    }

    builder = getBuilder.apply(application);
    stringBuilder = getStringBuilder.apply(application);
    createApplication();
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
    builder.stop();

    if (thread != null) {
      tryToDoRethrow(thread::join);
    }

    LOGGER.log(INFO, "Stopped {0}", new Object[] {name()});
  }

  private Publisher<Message<String, JsonObject>> toStream(
      final Publisher<Message<String, JsonObject>> stream, final JsonObject specification) {
    if (specification.containsKey(TO_TOPIC)) {
      connectToTopic(specification.getString(TO_TOPIC), specification);
    } else if (specification.containsKey(TO_COLLECTION)) {
      connectToCollection(stream, specification.getString(TO_COLLECTION));
    } else {
      pullForever(stream);
    }

    return stream;
  }

  private UnaryOperator<JsonObject> transformer(final String jslt) {
    final var op =
        fatal(
            () ->
                transformerObject(
                    new Jslt.Context(tryReader(jslt))
                        .withResolver(context.features.jsltResolver)
                        .withFunctions(context.features.customJsltFunctions)),
            LOGGER,
            () -> jslt);

    return json ->
        fatal(
            () -> op.apply(json),
            LOGGER,
            () -> "Script:\n" + numberLines(jslt) + "\n\nWith JSON:\n" + string(json, true));
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
                    .thenApply(m::withValue))
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
    return new App<>(
        specification, getBuilder, getStringBuilder, messageLag, new OnError(onError), context);
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

  private static class OnError {
    private Throwable error;
    private final Consumer<Throwable> onErrorFn;

    private OnError(final Consumer<Throwable> onError) {
      this.onErrorFn = onError;
    }

    private void clear() {
      error = null;
    }

    private void setError(final Throwable t) {
      if (error == null) {
        error = t;
        onErrorFn.accept(t);
      }
    }
  }

  private static class OtherSide {
    private final MongoCollection<Document> collection;
    private final JsonValue criterion;
    private final String name;

    private OtherSide(
        final String name, final MongoCollection<Document> collection, final JsonValue criterion) {
      this.name = name;
      this.collection = collection;
      this.criterion = criterion;
    }
  }

  private static class ThisSide {
    private final MongoCollection<Document> collection;
    private final Function<JsonObject, JsonValue> key;
    private final String name;

    private ThisSide(
        final String name,
        final MongoCollection<Document> collection,
        final Function<JsonObject, JsonValue> key) {
      this.name = name;
      this.collection = collection;
      this.key = key;
    }
  }
}
