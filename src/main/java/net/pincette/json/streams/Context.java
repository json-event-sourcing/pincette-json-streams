package net.pincette.json.streams;

import com.mongodb.reactivestreams.client.MongoDatabase;
import com.typesafe.config.Config;
import java.util.logging.Level;
import net.pincette.mongo.Features;

class Context {
  Config config;
  MongoDatabase database;
  String environment;
  Features features;
  Level logLevel;
}
