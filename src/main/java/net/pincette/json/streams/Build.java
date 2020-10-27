package net.pincette.json.streams;

import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.streams.Common.DOLLAR;
import static net.pincette.json.streams.Common.DOT;
import static net.pincette.json.streams.Common.SLASH;
import static net.pincette.json.streams.Common.build;
import static net.pincette.json.streams.Common.createTopologyContext;
import static net.pincette.json.streams.Common.getTopologyCollection;
import static net.pincette.json.streams.Common.readTopologies;
import static net.pincette.json.streams.Common.transformFieldNames;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.util.Util.must;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.io.File;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import org.bson.Document;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "build",
    description = "Generates one inlined JSON per topology and uploads them to MongoDB.")
class Build implements Runnable {
  private final Context context;

  @Option(
      names = {"-f", "--file"},
      required = true,
      description = "A JSON file containing an array of topologies.")
  private File file;

  @Option(
      names = {"-c", "--collection"},
      description = "The MongoDB collection the topologies are uploaded to.")
  private String collection;

  Build(final Context context) {
    this.context = context;
  }

  private static String escapeFieldName(final String name) {
    return name.replace(".", DOT).replace("/", SLASH).replace("$", DOLLAR);
  }

  private static JsonObject toMongoDB(final JsonObject json) {
    return transformFieldNames(json, Build::escapeFieldName);
  }

  private JsonArray buildTopologies() {
    return readTopologies(file)
        .map(
            specification ->
                build(
                    specification,
                    createTopologyContext(
                        specification, file.getAbsoluteFile().getParentFile(), context)))
        .filter(Common::validateTopology)
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
        .build();
  }

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    final String col = getTopologyCollection(collection, context);

    if (col != null) {
      final MongoCollection<Document> c = context.database.getCollection(col);

      buildTopologies().stream()
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
      System.out.println(string(buildTopologies()));
    }
  }
}
