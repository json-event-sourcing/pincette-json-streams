package net.pincette.json.streams;

import static java.util.UUID.randomUUID;

import com.mongodb.reactivestreams.client.MongoClient;
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
  MongoClient client;
  MongoDatabase database;
  MongoDatabase databaseArchive;
  String environment;
  Features features = new Features();
  String instance = randomUUID().toString();
  Level logLevel;
  String logTopic;
  Logger logger;
  KafkaProducer<String, JsonObject> producer;
  Map<String, Stage> stageExtensions;
}
