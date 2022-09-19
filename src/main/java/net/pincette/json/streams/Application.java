package net.pincette.json.streams;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static com.typesafe.config.ConfigFactory.defaultOverrides;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.json.streams.Common.createContext;
import static net.pincette.json.streams.KafkaProvider.tester;
import static net.pincette.json.streams.Logging.init;
import static net.pincette.util.Util.tryToDoWithRethrow;

import java.util.Optional;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    mixinStandardHelpOptions = true,
    version = Application.APP_VERSION,
    description = "The JSON Streams command-line interface.")
public class Application {
  static final String APP_VERSION = "2.2";
  private static final String MONGODB_URI = "mongodb.uri";

  private Application() {}

  public static void main(final String[] args) {
    final var config = AWSSecrets.load(defaultOverrides().withFallback(loadDefault()));

    init(config);

    tryToDoWithRethrow(
        () -> create(config.getString(MONGODB_URI)),
        client ->
            Optional.of(createContext(config, client))
                .map(
                    context ->
                        new CommandLine(new Application())
                            .addSubcommand("build", new Build(context))
                            .addSubcommand("delete", new Delete(context))
                            .addSubcommand("doc", new Doc(context))
                            .addSubcommand("dot", new Dot(context))
                            .addSubcommand(new HelpCommand())
                            .addSubcommand("list", new ListApps(context))
                            .addSubcommand(
                                "run", new Run<>(() -> new KafkaProvider(context.config), context))
                            .addSubcommand(
                                "test",
                                new Test<>(
                                    () -> new KafkaProvider(context.config),
                                    () -> tester(config),
                                    context))
                            .addSubcommand("yaml", new Yaml())
                            .execute(args))
                .filter(code -> code != 0)
                .ifPresent(System::exit));
  }
}
