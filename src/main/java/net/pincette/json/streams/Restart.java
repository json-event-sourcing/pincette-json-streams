package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.time.Instant.now;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
import static net.pincette.jes.JsonFields.TIMESTAMP;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.getApplicationCollection;
import static net.pincette.mongo.Collection.updateOne;
import static net.pincette.util.Util.must;

import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import java.util.function.Supplier;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "restart",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Restarts the application.")
class Restart implements Runnable {
  private final Supplier<Context> contextSupplier;

  @Option(
      names = {"-a", "--application"},
      required = true,
      description = "An application from the MongoDB collection containing the applications.")
  private String application;

  @Option(
      names = {"-c", "--collection"},
      description = "The MongoDB collection the application is restarted in.")
  private String collection;

  Restart(final Supplier<Context> contextSupplier) {
    this.contextSupplier = contextSupplier;
  }

  private UpdateResult logExists(final UpdateResult result) {
    if (result.getModifiedCount() != 1) {
      getGlobal().log(SEVERE, "The application {0} doesn''t exist.", application);
    }

    return result;
  }

  public void run() {
    final var context = contextSupplier.get();

    updateOne(
            context.database.getCollection(getApplicationCollection(collection, context)),
            eq(APPLICATION_FIELD, application),
            Updates.set(TIMESTAMP, now().toString()))
        .thenApply(result -> must(result, UpdateResult::wasAcknowledged))
        .thenApply(this::logExists)
        .toCompletableFuture()
        .join();
  }
}
