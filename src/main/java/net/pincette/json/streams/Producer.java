package net.pincette.json.streams;

import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.json.streams.Common.BACKOFF;
import static net.pincette.json.streams.Logging.exception;
import static net.pincette.util.Util.tryToGetForever;

import com.typesafe.config.Config;
import java.util.concurrent.CompletionStage;
import javax.json.JsonObject;
import net.pincette.jes.util.Kafka;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.rs.streams.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

class Producer implements AutoCloseable {
  private static final String KAFKA = "kafka";

  private final Config config;
  private KafkaProducer<String, JsonObject> jsonProducer;
  private KafkaProducer<String, String> stringProducer;

  Producer(final Config config) {
    this.config = config;
  }

  public void close() {
    if (jsonProducer != null) {
      jsonProducer.close();
    }

    if (stringProducer != null) {
      stringProducer.close();
    }
  }

  KafkaProducer<String, JsonObject> getJsonProducer() {
    if (jsonProducer == null) {
      jsonProducer = getNewJsonProducer();
    }

    return jsonProducer;
  }

  KafkaProducer<String, JsonObject> getNewJsonProducer() {
    return createReliableProducer(
        fromConfig(config, KAFKA), new StringSerializer(), new JsonSerializer());
  }

  KafkaProducer<String, String> getNewStringProducer() {
    return createReliableProducer(
        fromConfig(config, KAFKA), new StringSerializer(), new StringSerializer());
  }

  KafkaProducer<String, String> getStringProducer() {
    if (stringProducer == null) {
      stringProducer = getNewStringProducer();
    }

    return stringProducer;
  }

  CompletionStage<Boolean> sendJson(final String topic, final Message<String, JsonObject> message) {
    return tryToGetForever(
        () ->
            Kafka.send(getJsonProducer(), new ProducerRecord<>(topic, message.key, message.value)),
        BACKOFF,
        e -> {
          exception(e);

          if (jsonProducer != null) {
            jsonProducer.close();
            jsonProducer = null;
          }
        });
  }

  CompletionStage<Boolean> sendString(final String topic, final Message<String, String> message) {
    return tryToGetForever(
        () ->
            Kafka.send(
                getStringProducer(), new ProducerRecord<>(topic, message.key, message.value)),
        BACKOFF,
        e -> {
          exception(e);

          if (stringProducer != null) {
            stringProducer.close();
            stringProducer = null;
          }
        });
  }
}
