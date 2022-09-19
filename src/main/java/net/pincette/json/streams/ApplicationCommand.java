package net.pincette.json.streams;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
import static net.pincette.json.streams.Common.application;
import static net.pincette.json.streams.Common.build;
import static net.pincette.json.streams.Common.createApplicationContext;
import static net.pincette.json.streams.Validate.validateApplication;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToGetRethrow;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.io.DontCloseOutputStream;
import org.bson.Document;
import org.bson.conversions.Bson;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

class ApplicationCommand {
  protected final Context context;

  @Option(
      names = {"-e", "--exclude"},
      arity = "1..*",
      description = "Excluded applications")
  private String[] excluded;

  @ArgGroup() private FileOrApplication fileOrApplication;

  ApplicationCommand(final Context context) {
    this.context = context;
  }

  private Bson applicationFilter() {
    return ofNullable(fileOrApplication)
        .map(f -> f.application)
        .map(a -> a.application)
        .map(a -> eq(APPLICATION_FIELD, a))
        .orElseGet(() -> exists(APPLICATION_FIELD));
  }

  private Optional<String> getCollection() {
    return ofNullable(fileOrApplication).map(f -> f.application).map(a -> a.collection);
  }

  private Optional<File> getFile() {
    return Optional.ofNullable(fileOrApplication).map(f -> f.file).map(f -> f.file);
  }

  private Stream<Loaded> getApplications() {
    final Set<String> excludedApplications =
        ofNullable(excluded).stream().flatMap(Arrays::stream).collect(toSet());

    return getFile()
        .map(Read::readTopologies)
        .orElseGet(
            () ->
                Common.getApplications(getApplicationCollection(), applicationFilter())
                    .map(Loaded::new))
        .filter(loaded -> !excludedApplications.contains(application(loaded.specification)));
  }

  private MongoCollection<Document> getApplicationCollection() {
    return context.database.getCollection(
        Common.getApplicationCollection(getCollection().orElse(null), context));
  }

  protected Stream<JsonObject> getValidatedApplications() {
    return getApplications()
        .map(
            loaded ->
                pair(
                    loaded.specification,
                    createApplicationContext(loaded, getFile().orElse(null), context)))
        .map(pair -> pair(build(pair.first, false, pair.second), pair.second))
        .filter(pair -> validateApplication(pair.first))
        .map(pair -> pair.first);
  }

  @SuppressWarnings("java:S106") // Not logging.
  protected PrintWriter getWriter(final String filename) {
    return ofNullable(fileOrApplication)
        .map(f -> f.application)
        .map(a -> a.directory)
        .map(
            directory ->
                new PrintWriter(
                    requireNonNull(
                        tryToGetRethrow(() -> new FileWriter(new File(directory, filename)))
                            .orElse(null))))
        .orElseGet(
            () ->
                new PrintWriter(
                    new OutputStreamWriter(new DontCloseOutputStream(System.out), UTF_8)));
  }

  private static class FileOrApplication {
    @ArgGroup(exclusive = false)
    private ApplicationOptions application;

    @ArgGroup(exclusive = false)
    private FileOptions file;

    private static class ApplicationOptions {
      @Option(
          names = {"-a", "--application"},
          description = "An application from the MongoDB collection containing the applications.")
      private String application;

      @Option(
          names = {"-c", "--collection"},
          description =
              "A MongoDB collection containing the applications. The configuration may "
                  + "define a default collection.")
      private String collection;

      @Option(
          names = {"-d", "--directory"},
          description = "A directory where the results for each application are saved.")
      private File directory;
    }

    private static class FileOptions {
      @Option(
          names = {"-f", "--file"},
          required = true,
          description = "A JSON or YAML file containing an application object.")
      private File file;
    }
  }
}
