package net.pincette.json.streams;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import javax.json.JsonObject;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

class TopologyContext {
  final String application;
  final File baseDirectory;
  final StreamsBuilder builder = new StreamsBuilder();
  final Map<String, JsonObject> configurations = new HashMap<>();
  final Context context;
  final Map<String, KStream<String, JsonObject>> streams = new HashMap<>();

  TopologyContext() {
    this(null, null, null);
  }

  private TopologyContext(
      final String application, final File baseDirectory, final Context context) {
    this.application = application;
    this.baseDirectory = baseDirectory;
    this.context = context;
  }

  TopologyContext withApplication(final String application) {
    return new TopologyContext(application, baseDirectory, context);
  }

  TopologyContext withBaseDirectory(final File baseDirectory) {
    return new TopologyContext(application, baseDirectory, context);
  }

  TopologyContext withContext(final Context context) {
    return new TopologyContext(application, baseDirectory, context);
  }
}
