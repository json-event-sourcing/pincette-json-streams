package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.createContext;
import static net.pincette.json.streams.Common.getApplicationCollection;
import static net.pincette.json.streams.Common.overrideConfig;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.util.Util.must;

import com.mongodb.client.result.DeleteResult;
import com.typesafe.config.Config;
import java.io.File;
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
  private final Config config;

  @Option(
      names = {"-a", "--application"},
      required = true,
      description = "An application from the MongoDB collection containing the applications.")
  private String application;

  @Option(
      names = {"-c", "--collection"},
      description = "The MongoDB collection the application is deleted from.")
  private String collection;

  @Option(
      names = {"-C", "--config"},
      description = "The configuration with overrides to the default one.")
  private File configFile;

  Delete(final Config config) {
    this.config = config;
  }

  private DeleteResult logExists(final DeleteResult result) {
    if (result.getDeletedCount() != 1) {
      getGlobal().log(SEVERE, "The application {0} doesn''t exist.", application);
    }

    return result;
  }

  public void run() {
    final var context = createContext(overrideConfig(config, configFile));

    deleteOne(
            context.database.getCollection(getApplicationCollection(collection, context)),
            eq(APPLICATION_FIELD, application))
        .thenApply(result -> must(result, DeleteResult::wasAcknowledged))
        .thenApply(this::logExists)
        .toCompletableFuture()
        .join();
  }
}
