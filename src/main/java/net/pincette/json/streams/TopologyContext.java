package net.pincette.json.streams;

import com.schibsted.spt.data.jslt.ResourceResolver;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import javax.json.JsonObject;
import net.pincette.mongo.Validator;
import net.pincette.mongo.Validator.Resolver;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

class TopologyContext {
  final String application;
  final File baseDirectory;
  final StreamsBuilder builder = new StreamsBuilder();
  final Map<String, JsonObject> configurations = new HashMap<>();
  final Context context;
  final Map<String, KStream<String, JsonObject>> streams = new HashMap<>();
  final Validator validators;
  private final Resolver validatorResolver;

  TopologyContext(final Context context) {
    this(null, null, null, null, context);
  }

  private TopologyContext(
      final String application,
      final File baseDirectory,
      final ResourceResolver jsltResolver,
      final Resolver validatorResolver,
      final Context context) {
    this.application = application;
    this.baseDirectory = baseDirectory;
    this.context = context;
    context.features = context.features.withJsltResolver(jsltResolver);
    this.validatorResolver = validatorResolver;
    validators = new Validator(context.features, validatorResolver);
  }

  TopologyContext withApplication(final String application) {
    return new TopologyContext(
        application, baseDirectory, context.features.jsltResolver, validatorResolver, context);
  }

  TopologyContext withBaseDirectory(final File baseDirectory) {
    return new TopologyContext(
        application, baseDirectory, context.features.jsltResolver, validatorResolver, context);
  }

  TopologyContext withJsltResolver(final ResourceResolver resolver) {
    return new TopologyContext(application, baseDirectory, resolver, validatorResolver, context);
  }

  TopologyContext withValidatorResolver(final Resolver resolver) {
    return new TopologyContext(
        application, baseDirectory, context.features.jsltResolver, resolver, context);
  }
}
