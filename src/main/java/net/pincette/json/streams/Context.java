package net.pincette.json.streams;

import com.mongodb.reactivestreams.client.MongoDatabase;
import com.typesafe.config.Config;
import java.util.logging.Level;
import javax.json.JsonObject;
import net.pincette.mongo.Features;
import org.apache.kafka.clients.producer.KafkaProducer;

class Context {
  Config config;
  MongoDatabase database;
  String environment;
  Features features;
  Level logLevel;
  KafkaProducer<String, JsonObject> producer;
}
