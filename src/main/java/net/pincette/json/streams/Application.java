package net.pincette.json.streams;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.lang.ClassLoader.getSystemResourceAsStream;
import static java.lang.System.getProperty;
import static java.util.logging.Level.parse;
import static java.util.logging.LogManager.getLogManager;
import static java.util.logging.Logger.getLogger;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.json.streams.Common.ENVIRONMENT;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    mixinStandardHelpOptions = true,
    version = Application.APP_VERSION,
    description = "The JSON Streams command-line interface.")
public class Application {
  static final String APP_VERSION = "2.0";
  private static final String DATABASE = "mongodb.database";
  private static final String LOGGER = "pincette-json-streams";
  private static final String LOG_LEVEL = "logLevel";
  private static final String LOG_TOPIC = "logTopic";
  private static final String MONGODB_URI = "mongodb.uri";
  private static final String MONGODB_URI_ARCHIVE = "mongodb.uriArchive";

  private Application() {}

  private static MongoClient getArchiveClient(final Config config) {
    return tryToGetSilent(() -> config.getString(MONGODB_URI_ARCHIVE))
        .map(MongoClients::create)
        .orElse(null);
  }

  private static void initLogging(final Level level) {
    if (getProperty("java.util.logging.config.class") == null
        && getProperty("java.util.logging.config.file") == null) {
      final var logging = loadLogging();
      final var out = new ByteArrayOutputStream();

      logging.setProperty(".level", level.getName());

      tryToDoRethrow(
          () -> {
            logging.store(out, null);
            getLogManager().readConfiguration(new ByteArrayInputStream(out.toByteArray()));
          });
    }
  }

  private static Properties loadLogging() {
    final var properties = new Properties();

    tryToDoRethrow(() -> properties.load(getSystemResourceAsStream("logging.properties")));

    return properties;
  }

  public static void main(final String[] args) {
    final var context = new Context();

    context.logger = getLogger(LOGGER);
    context.config = loadDefault();
    context.environment = tryToGetSilent(() -> context.config.getString(ENVIRONMENT)).orElse(null);
    context.logLevel =
        parse(tryToGetSilent(() -> context.config.getString(LOG_LEVEL)).orElse("SEVERE"));
    context.logTopic = tryToGetSilent(() -> context.config.getString(LOG_TOPIC)).orElse(null);
    context.logger.setLevel(context.logLevel);
    initLogging(context.logLevel);

    try (final var client = create(context.config.getString(MONGODB_URI));
        final var archiveClient = getArchiveClient(context.config)) {
      context.client = client;
      context.database = client.getDatabase(context.config.getString(DATABASE));

      if (archiveClient != null) {
        context.databaseArchive = archiveClient.getDatabase(context.config.getString(DATABASE));
      }

      Optional.of(
              new CommandLine(new Application())
                  .addSubcommand("build", new Build(context))
                  .addSubcommand("delete", new Delete(context))
                  .addSubcommand("doc", new Doc(context))
                  .addSubcommand("dot", new Dot(context))
                  .addSubcommand(new HelpCommand())
                  .addSubcommand("list", new ListApps(context))
                  .addSubcommand("run", new Run(context))
                  .addSubcommand("yaml", new Yaml())
                  .execute(args))
          .filter(code -> code != 0)
          .ifPresent(System::exit);
    }
  }
}
