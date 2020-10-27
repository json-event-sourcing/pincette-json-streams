package net.pincette.json.streams;

import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.VERSION;
import static net.pincette.json.streams.Common.getTopologyCollection;
import static net.pincette.mongo.JsonClient.aggregate;
import static net.pincette.util.Collections.list;

import java.util.List;
import javax.json.JsonObject;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "list", description = "Lists the running applications.")
class ListApps implements Runnable {
  private final Context context;

  @Option(
      names = {"-c", "--collection"},
      description = "The MongoDB collection that contains the topologies.")
  private String collection;

  ListApps(final Context context) {
    this.context = context;
  }

  @SuppressWarnings("java:S106") // Not logging.
  private static List<JsonObject> print(final List<JsonObject> result) {
    result.stream()
        .map(r -> r.getString(APPLICATION_FIELD, null) + " " + r.getString(VERSION, null))
        .sorted()
        .forEach(System.out::println);

    return result;
  }

  public void run() {
    aggregate(
            context.database.getCollection(getTopologyCollection(collection, context)),
            list(project(fields(include(APPLICATION_FIELD, VERSION)))))
        .thenApply(ListApps::print)
        .toCompletableFuture()
        .join();
  }
}
