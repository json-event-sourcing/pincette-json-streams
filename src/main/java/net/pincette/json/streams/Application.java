package net.pincette.json.streams;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.util.logging.Level.parse;
import static java.util.logging.Logger.getLogger;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.jes.util.MongoExpressions.operators;
import static net.pincette.json.Jslt.customFunctions;
import static net.pincette.json.Jslt.registerCustomFunctions;
import static net.pincette.json.Jslt.trace;
import static net.pincette.json.streams.Common.ENVIRONMENT;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Collections.union;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import java.util.Optional;
import java.util.logging.Logger;
import net.pincette.mongo.Features;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command
public class Application {
  private static final String DATABASE = "mongodb.database";
  private static final String LOGGER = "pincette-json-streams";
  private static final String LOG_LEVEL = "logLevel";
  private static final String MONGODB_URI = "mongodb.uri";

  private Application() {}

  public static void main(final String[] args) {
    final Context context = new Context();
    final Logger logger = getLogger(LOGGER);

    context.config = loadDefault();
    context.environment = tryToGetSilent(() -> context.config.getString(ENVIRONMENT)).orElse(null);
    context.logLevel =
        parse(tryToGetSilent(() -> context.config.getString(LOG_LEVEL)).orElse("INFO"));

    logger.setLevel(context.logLevel);
    registerCustomFunctions(union(customFunctions(), set(trace(logger))));

    tryToDoWithRethrow(
        () -> create(context.config.getString(MONGODB_URI)),
        client -> {
          context.database = client.getDatabase(context.config.getString(DATABASE));
          context.features =
              new Features()
                  .withExpressionExtensions(operators(context.database, context.environment));

          Optional.of(
                  new CommandLine(new Application())
                      .addSubcommand("build", new Build(context))
                      .addSubcommand("delete", new Delete(context))
                      .addSubcommand("list", new ListApps(context))
                      .addSubcommand("run", new Run(context))
                      .execute(args))
              .filter(code -> code != 0)
              .ifPresent(System::exit);
        });
  }
}
