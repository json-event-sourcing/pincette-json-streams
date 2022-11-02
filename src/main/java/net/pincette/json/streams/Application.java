package net.pincette.json.streams;

import static com.typesafe.config.ConfigFactory.defaultOverrides;
import static java.lang.System.exit;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.json.streams.Common.createContext;
import static net.pincette.json.streams.KafkaProvider.tester;
import static net.pincette.json.streams.Logging.LOGGER_NAME;
import static net.pincette.json.streams.Logging.getLogger;
import static net.pincette.json.streams.Logging.init;

import com.typesafe.config.Config;
import java.util.function.Supplier;
import java.util.logging.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    mixinStandardHelpOptions = true,
    version = Application.APP_VERSION,
    description = "The JSON Streams command-line interface.")
public class Application {
  static final String APP_VERSION = "2.2.6";
  private static final Logger CONFIG_LOGGER = getLogger(LOGGER_NAME + ".config");

  private Application() {}

  private static Supplier<Context> awsContext(final Config config) {
    return () -> createContext(AWSSecrets.load(config));
  }

  public static void main(final String[] args) {
    final var config = defaultOverrides().withFallback(loadDefault());

    init(config);
    Logging.trace(() -> "Loaded config: ", config, CONFIG_LOGGER);

    exit(
        new CommandLine(new Application())
            .addSubcommand("build", new Build(awsContext(config)))
            .addSubcommand("delete", new Delete(awsContext(config)))
            .addSubcommand("doc", new Doc(awsContext(config)))
            .addSubcommand("dot", new Dot(awsContext(config)))
            .addSubcommand(new HelpCommand())
            .addSubcommand("list", new ListApps(awsContext(config)))
            .addSubcommand("restart", new Restart(awsContext(config)))
            .addSubcommand("run", new Run<>(() -> new KafkaProvider(config), awsContext(config)))
            .addSubcommand(
                "test",
                new Test<>(
                    () -> new KafkaProvider(config),
                    () -> tester(config),
                    () -> createContext(config)))
            .addSubcommand("yaml", new Yaml())
            .execute(args));
  }
}
