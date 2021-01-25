package net.pincette.json.streams;

import com.mongodb.reactivestreams.client.MongoDatabase;
import com.typesafe.config.Config;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.JsonObject;
import net.pincette.mongo.Features;
import net.pincette.mongo.streams.Stage;
import org.apache.kafka.clients.producer.KafkaProducer;

class Context {
  Config config;
  MongoDatabase database;
  MongoDatabase databaseArchive;
  String environment;
  Features features;
  Level logLevel;
  String logTopic;
  Logger logger;
  KafkaProducer<String, JsonObject> producer;
  Map<String, Stage> stageExtensions;
}
