package net.pincette.json.streams;

import static java.lang.System.exit;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.json.streams.Logging.LOGGER_NAME;
import static net.pincette.json.streams.Logging.getLogger;
import static net.pincette.json.streams.Logging.init;

import java.util.logging.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    mixinStandardHelpOptions = true,
    version = Application.APP_VERSION,
    description = "The JSON Streams command-line interface.")
public class Application {
  static final String APP_VERSION = "2.9.0";
  private static final Logger CONFIG_LOGGER = getLogger(LOGGER_NAME + ".config");

  private Application() {}

  public static void main(final String[] args) {
    final var config = loadDefault();

    init(config);
    Logging.trace(() -> "Loaded config: ", config, CONFIG_LOGGER);

    exit(
        new CommandLine(new Application())
            .addSubcommand("build", new Build(config))
            .addSubcommand("delete", new Delete(config))
            .addSubcommand("doc", new Doc(config))
            .addSubcommand("dot", new Dot(config))
            .addSubcommand(new HelpCommand())
            .addSubcommand("list", new ListApps(config))
            .addSubcommand("restart", new Restart(config))
            .addSubcommand("run", new Run<>(config, KafkaProvider::new, Common::createContext))
            .addSubcommand("test", new Test<>(config, KafkaProvider::tester))
            .addSubcommand("yaml", new Yaml())
            .execute(args));
  }
}
