package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.getTopologyCollection;
import static net.pincette.mongo.Collection.deleteOne;
import static net.pincette.util.Util.must;

import com.mongodb.client.result.DeleteResult;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "delete",
    description = "Removes the topology from the MongoDB collection, which causes it to stop.")
class Delete implements Runnable {
  private final Context context;

  @Option(
      names = {"-a", "--application"},
      required = true,
      description = "An application from the MongoDB collection containing the topologies.")
  private String application;

  @Option(
      names = {"-c", "--collection"},
      description = "The MongoDB collection the topology is deleted from.")
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
            context.database.getCollection(getTopologyCollection(collection, context)),
            eq(APPLICATION_FIELD, application))
        .thenApply(result -> must(result, DeleteResult::wasAcknowledged))
        .thenApply(this::logExists)
        .toCompletableFuture()
        .join();
  }
}
