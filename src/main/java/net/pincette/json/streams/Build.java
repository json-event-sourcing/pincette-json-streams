package net.pincette.json.streams;

import static java.time.Instant.now;
import static net.pincette.jes.JsonFields.TIMESTAMP;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Common.DOLLAR;
import static net.pincette.json.streams.Common.DOT;
import static net.pincette.json.streams.Common.SLASH;
import static net.pincette.json.streams.Common.build;
import static net.pincette.json.streams.Common.createApplicationContext;
import static net.pincette.json.streams.Common.getApplicationCollection;
import static net.pincette.json.streams.Common.transformFieldNames;
import static net.pincette.json.streams.Read.readTopologies;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.util.Util.must;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.io.File;
import java.util.function.Supplier;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import org.bson.Document;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "build",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Generates one inlined JSON per application and uploads them to MongoDB.")
class Build implements Runnable {
  private final Supplier<Context> contextSupplier;
  @ArgGroup() private CollectionOrLocal collectionOrLocal;

  @Option(
      names = {"-f", "--file"},
      required = true,
      description = "A JSON or YAML file containing an array of applications.")
  private File file;

  Build(final Supplier<Context> contextSupplier) {
    this.contextSupplier = contextSupplier;
  }

  private static String escapeFieldName(final String name) {
    return name.replace(".", DOT).replace("/", SLASH).replace("$", DOLLAR);
  }

  private static JsonObject toMongoDB(final JsonObject json) {
    return transformFieldNames(json, Build::escapeFieldName)
        .add(TIMESTAMP, now().toString())
        .build();
  }

  private JsonArray buildApplications(final Context context) {
    return readTopologies(file)
        .map(
            loaded ->
                build(loaded.specification, false, createApplicationContext(loaded, file, context)))
        .filter(Validate::validateApplication)
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
        .build();
  }

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    final var context = contextSupplier.get();
    final var col =
        getApplicationCollection(
            collectionOrLocal == null ? null : collectionOrLocal.collection, context);

    if ((collectionOrLocal == null || !collectionOrLocal.local) && col != null) {
      final MongoCollection<Document> c = context.database.getCollection(col);

      buildApplications(context).stream()
          .filter(JsonUtil::isObject)
          .map(JsonValue::asJsonObject)
          .map(Build::toMongoDB)
          .forEach(
              t ->
                  update(c, t)
                      .thenApply(result -> must(result, r -> r))
                      .toCompletableFuture()
                      .join());
    } else {
      System.out.println(string(buildApplications(context), true));
    }
  }

  private static class CollectionOrLocal {
    @Option(
        names = {"-c", "--collection"},
        description = "The MongoDB collection the topologies are uploaded to.")
    private String collection;

    @Option(
        names = {"-l", "--local"},
        description =
            "Sends the build result to stdout, even when the configuration contains a "
                + "topology collection.")
    private boolean local;
  }
}
