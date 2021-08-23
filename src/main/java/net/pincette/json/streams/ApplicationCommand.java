package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static java.util.Optional.ofNullable;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.build;
import static net.pincette.json.streams.Common.createTopologyContext;
import static net.pincette.json.streams.Common.getTopologies;
import static net.pincette.json.streams.Validate.validateTopology;
import static net.pincette.util.Pair.pair;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.io.File;
import java.util.Optional;
import java.util.stream.Stream;
import javax.json.JsonObject;
import org.bson.Document;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

class ApplicationCommand {
  protected final Context context;
  @ArgGroup() private FileOrApplication fileOrApplication;

  ApplicationCommand(final Context context) {
    this.context = context;
  }

  private Optional<String> getCollection() {
    return ofNullable(fileOrApplication).map(f -> f.application).map(a -> a.collection);
  }

  private Optional<File> getFile() {
    return Optional.ofNullable(fileOrApplication).map(f -> f.file).map(f -> f.file);
  }

  private Optional<Loaded> getTopology() {
    return getFile()
        .map(Read::readTopologies)
        .map(Stream::findFirst)
        .orElseGet(
            () ->
                getTopologies(
                        getTopologyCollection(),
                        eq(APPLICATION_FIELD, fileOrApplication.application.application))
                    .map(Loaded::new)
                    .findFirst());
  }

  private MongoCollection<Document> getTopologyCollection() {
    return context.database.getCollection(
        Common.getTopologyCollection(getCollection().orElse(null), context));
  }

  protected Optional<JsonObject> getValidatedTopology() {
    return getTopology()
        .map(
            loaded ->
                pair(
                    loaded.specification,
                    createTopologyContext(loaded, getFile().orElse(null), context)))
        .map(pair -> pair(build(pair.first, false, pair.second), pair.second))
        .filter(pair -> validateTopology(pair.first))
        .map(pair -> pair.first);
  }

  private static class FileOrApplication {
    @ArgGroup(exclusive = false)
    private ApplicationOptions application;

    @ArgGroup(exclusive = false)
    private FileOptions file;

    private static class ApplicationOptions {
      @Option(
          names = {"-a", "--application"},
          required = true,
          description = "An application from the MongoDB collection containing the applications.")
      private String application;

      @Option(
          names = {"-c", "--collection"},
          description =
              "A MongoDB collection containing the applications. The configuration may "
                  + "define a default collection.")
      private String collection;
    }

    private static class FileOptions {
      @Option(
          names = {"-f", "--file"},
          required = true,
          description = "A JSON file containing an application object.")
      private File file;
    }
  }
}
