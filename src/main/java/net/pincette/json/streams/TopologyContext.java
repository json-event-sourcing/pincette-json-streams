package net.pincette.json.streams;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import javax.json.JsonObject;
import net.pincette.mongo.Features;
import net.pincette.mongo.Validator;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

class TopologyContext {
  final StreamsBuilder builder = new StreamsBuilder();
  final Map<String, JsonObject> configurations = new HashMap<>();
  final Context context;
  final Map<String, KStream<String, JsonObject>> streams = new HashMap<>();
  final Validator validators = new Validator();
  String application;
  File baseDirectory;
  Features features;

  TopologyContext(final Context context) {
    this.context = context;
  }
}
