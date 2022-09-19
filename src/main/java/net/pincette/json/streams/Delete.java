package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.getApplicationCollection;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.util.Util.must;

import com.mongodb.client.result.DeleteResult;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "delete",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Removes the application from the MongoDB collection, which causes it to stop.")
class Delete implements Runnable {
  private final Context context;

  @Option(
      names = {"-a", "--application"},
      required = true,
      description = "An application from the MongoDB collection containing the applications.")
  private String application;

  @Option(
      names = {"-c", "--collection"},
      description = "The MongoDB collection the application is deleted from.")
  private String collection;

  Delete(final Context context) {
    this.context = context;
  }

  private DeleteResult logExists(final DeleteResult result) {
    if (result.getDeletedCount() != 1) {
      getGlobal().log(SEVERE, "The application {0} doesn''t exist.", application);
    }

    return result;
  }

  public void run() {
    deleteOne(
            context.database.getCollection(getApplicationCollection(collection, context)),
            eq(APPLICATION_FIELD, application))
        .thenApply(result -> must(result, DeleteResult::wasAcknowledged))
        .thenApply(this::logExists)
        .toCompletableFuture()
        .join();
  }
}
