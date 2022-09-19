package net.pincette.json.streams;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.VERSION_FIELD;
import static net.pincette.json.streams.Common.application;
import static net.pincette.json.streams.Common.getApplicationCollection;
import static net.pincette.mongo.JsonClient.aggregate;
import static net.pincette.util.Collections.list;

import java.util.List;
import javax.json.JsonObject;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "list",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Lists the running applications.")
class ListApps implements Runnable {
  private final Context context;

  @Option(
      names = {"-c", "--collection"},
      description = "The MongoDB collection that contains the applications.")
  private String collection;

  ListApps(final Context context) {
    this.context = context;
  }

  @SuppressWarnings("java:S106") // Not logging.
  private static List<JsonObject> print(final List<JsonObject> result) {
    result.stream()
        .map(r -> application(r) + " " + r.getString(VERSION_FIELD, null))
        .sorted()
        .forEach(System.out::println);

    return result;
  }

  public void run() {
    aggregate(
            context.database.getCollection(getApplicationCollection(collection, context)),
            list(
                match(exists(APPLICATION_FIELD)),
                project(fields(include(APPLICATION_FIELD, VERSION_FIELD)))))
        .thenApply(ListApps::print)
        .toCompletableFuture()
        .join();
  }
}
