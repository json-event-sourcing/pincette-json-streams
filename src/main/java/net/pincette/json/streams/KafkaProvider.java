package net.pincette.json.streams;

import static java.time.Duration.ofMillis;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedStage;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static net.pincette.config.Util.configValue;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.json.streams.Common.configValueApp;
import static net.pincette.rs.kafka.ConsumerEvent.STARTED;
import static net.pincette.rs.kafka.KafkaPublisher.publisher;
import static net.pincette.rs.kafka.KafkaSubscriber.subscriber;
import static net.pincette.rs.kafka.Util.fromPublisher;
import static net.pincette.rs.kafka.Util.fromSubscriber;
import static net.pincette.rs.streams.Streams.streams;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Collections.union;
import static net.pincette.util.Pair.pair;
import static org.apache.kafka.clients.admin.AdminClientConfig.configNames;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import javax.json.JsonObject;
import net.pincette.jes.util.Kafka;
import net.pincette.kafka.json.JsonDeserializer;
import net.pincette.rs.kafka.ConsumerEvent;
import net.pincette.rs.kafka.ProducerEvent;
import net.pincette.rs.streams.Streams;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaProvider
    implements TestProvider<
        ConsumerRecord<String, JsonObject>,
        ProducerRecord<String, JsonObject>,
        ConsumerRecord<String, String>,
        ProducerRecord<String, String>> {
  private static final String BATCH_SIZE = "batchSize";
  private static final String BATCH_TIMEOUT = "batchTimeout";
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final Duration DEFAULT_BATCH_TIMEOUT = ofMillis(50);
  private static final String GROUP_ID_SUFFIX = "groupIdSuffix";
  private static final String KAFKA = "kafka";
  private static final Set<String> KAFKA_ADMIN_CONFIG_NAMES =
      union(
          configNames(),
          set("ssl.endpoint.identification.algorithm", "sasl.mechanism", "sasl.jaas.config"));
  private static final Map<String, Object> KAFKA_DEFAULT =
      map(
          pair(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
          pair(VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class),
          pair(ENABLE_AUTO_COMMIT_CONFIG, false));
  private static final String THROTTLE_TIME = "throttleTime";

  private final Admin admin;
  private final Config config;
  private final BiConsumer<ConsumerEvent, KafkaConsumer<String, JsonObject>> consumerEventHandler;
  private final String groupIdSuffix;
  private final Map<String, Object> kafkaConfig;
  private final Producer producer;
  private final BiConsumer<ProducerEvent, KafkaProducer<String, JsonObject>>
      producerEventHandlerJsonObject;
  private final BiConsumer<ProducerEvent, KafkaProducer<String, String>> producerEventHandlerString;

  KafkaProvider(final Config config) {
    this(config, new Producer(config));
  }

  private KafkaProvider(final Config config, final Producer producer) {
    this.config = config;
    this.producer = producer;
    kafkaConfig = fromConfig(config, KAFKA);
    admin = Admin.create(toAdmin(kafkaConfig));
    groupIdSuffix = configValue(config::getString, GROUP_ID_SUFFIX).orElse(null);
    consumerEventHandler = null;
    producerEventHandlerJsonObject = null;
    producerEventHandlerString = null;
  }

  private KafkaProvider(
      final KafkaProvider provider,
      final BiConsumer<ConsumerEvent, KafkaConsumer<String, JsonObject>> consumerEventHandler,
      final BiConsumer<ProducerEvent, KafkaProducer<String, JsonObject>>
          producerEventHandlerJsonObject,
      final BiConsumer<ProducerEvent, KafkaProducer<String, String>> producerEventHandlerString) {
    admin = provider.admin;
    config = provider.config;
    groupIdSuffix = provider.groupIdSuffix;
    kafkaConfig = provider.kafkaConfig;
    producer = provider.producer;
    this.consumerEventHandler = consumerEventHandler;
    this.producerEventHandlerJsonObject = producerEventHandlerJsonObject;
    this.producerEventHandlerString = producerEventHandlerString;
  }

  @SuppressWarnings("java:S2095") // That is up to the caller.
  static TestProvider<
          ConsumerRecord<String, JsonObject>,
          ProducerRecord<String, JsonObject>,
          ConsumerRecord<String, String>,
          ProducerRecord<String, String>>
      tester(final Config config) {
    return new KafkaProvider(config, new Producer(config))
        .withConsumerEventHandler(
            (e, c) -> {
              if (e == STARTED) {
                c.seekToBeginning(c.assignment());
              }
            });
  }

  private static Map<String, Object> toAdmin(final Map<String, Object> config) {
    return config.entrySet().stream()
        .filter(e -> KAFKA_ADMIN_CONFIG_NAMES.contains(e.getKey()))
        .collect(toMap(Entry::getKey, Entry::getValue));
  }

  private static Map<Partition, Long> toPartition(final Map<TopicPartition, Long> lags) {
    return lags.entrySet().stream()
        .collect(
            toMap(e -> new Partition(e.getKey().topic(), e.getKey().partition()), Entry::getValue));
  }

  public CompletionStage<Boolean> allAssigned(final String application, final Set<String> topics) {
    return topics.isEmpty()
        ? completedStage(true)
        : getAssigned(application).thenApply(t -> t.containsAll(topics));
  }

  private int batchSize(final String application) {
    return configValueApp(config::getInt, BATCH_SIZE, application).orElse(DEFAULT_BATCH_SIZE);
  }

  private Duration batchTimeout(final String application) {
    return configValueApp(config::getDuration, BATCH_TIMEOUT, application)
        .orElse(DEFAULT_BATCH_TIMEOUT);
  }

  public Streams<
          String,
          JsonObject,
          ConsumerRecord<String, JsonObject>,
          ProducerRecord<String, JsonObject>>
      builder(final String application) {
    final int batchSize = batchSize(application);

    return streams(
        fromPublisher(
            publisher(() -> consumer(application, batchSize))
                .withEventHandler(consumerEventHandler)
                .withThrottleTime(
                    configValueApp(config::getDuration, THROTTLE_TIME, application).orElse(null))),
        fromSubscriber(
            subscriber(producer::getNewJsonProducer)
                .withBatchSize(batchSize)
                .withTimeout(batchTimeout(application))
                .withEventHandler(producerEventHandlerJsonObject)));
  }

  public void close() {
    admin.close();
  }

  private KafkaConsumer<String, JsonObject> consumer(final String groupId, final int batchSize) {
    return new KafkaConsumer<>(
        merge(
            merge(kafkaConfig, KAFKA_DEFAULT),
            map(
                pair(
                    GROUP_ID_CONFIG,
                    groupId + (groupIdSuffix != null ? ("-" + groupIdSuffix) : "")),
                pair(MAX_POLL_RECORDS_CONFIG, batchSize))));
  }

  public void createTopics(final Set<String> topics) {
    net.pincette.rs.kafka.Util.createTopics(
            topics.stream().map(topic -> new NewTopic(topic, 1, (short) 1)).collect(toSet()), admin)
        .toCompletableFuture()
        .join();
  }

  @Override
  public void deleteConsumerGroups(final Set<String> groupIds) {
    net.pincette.rs.kafka.Util.deleteConsumerGroups(groupIds, admin).toCompletableFuture().join();
  }

  public void deleteTopics(final Set<String> topics) {
    net.pincette.rs.kafka.Util.deleteTopics(topics, admin).toCompletableFuture().join();
  }

  private CompletionStage<Set<String>> getAssigned(final String groupId) {
    return admin
        .describeConsumerGroups(set(groupId))
        .all()
        .toCompletionStage()
        .thenApply(
            result ->
                result.get(groupId).members().stream()
                    .map(MemberDescription::assignment)
                    .flatMap(a -> a.topicPartitions().stream())
                    .map(TopicPartition::topic)
                    .collect(toSet()))
        .exceptionally(t -> emptySet());
  }

  public Producer getProducer() {
    return producer;
  }

  public CompletionStage<Map<String, Map<Partition, Long>>> messageLag(
      final Predicate<String> includeGroup) {
    return Kafka.messageLag(admin, includeGroup)
        .thenApply(
            map ->
                map.entrySet().stream()
                    .collect(toMap(Entry::getKey, e -> toPartition(e.getValue()))));
  }

  public Streams<String, String, ConsumerRecord<String, String>, ProducerRecord<String, String>>
      stringBuilder(final String application) {
    return streams(
        null,
        fromSubscriber(
            subscriber(producer::getNewStringProducer)
                .withBatchSize(batchSize(application))
                .withTimeout(batchTimeout(application))
                .withEventHandler(producerEventHandlerString)));
  }

  KafkaProvider withConsumerEventHandler(
      final BiConsumer<ConsumerEvent, KafkaConsumer<String, JsonObject>> consumerEventHandler) {
    return new KafkaProvider(
        this, consumerEventHandler, producerEventHandlerJsonObject, producerEventHandlerString);
  }

  KafkaProvider withProducerEventHandlerJsonObject(
      final BiConsumer<ProducerEvent, KafkaProducer<String, JsonObject>>
          producerEventHandlerJsonObject) {
    return new KafkaProvider(
        this, consumerEventHandler, producerEventHandlerJsonObject, producerEventHandlerString);
  }

  KafkaProvider withProducerEventHandlerString(
      final BiConsumer<ProducerEvent, KafkaProducer<String, String>> producerEventHandlerString) {
    return new KafkaProvider(
        this, consumerEventHandler, producerEventHandlerJsonObject, producerEventHandlerString);
  }
}
