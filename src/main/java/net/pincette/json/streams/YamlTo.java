package net.pincette.json.streams;

import static net.pincette.json.JsonOrYaml.readJson;
import static net.pincette.json.JsonOrYaml.writeYaml;
import static net.pincette.json.streams.Application.APP_VERSION;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "to",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Convert JSON to YAML. It reads from stdin and writes to stdout.")
@SuppressWarnings("java:S106") // Not logging.
class YamlTo implements Runnable {
  public void run() {
    readJson(new InputStreamReader(System.in))
        .ifPresent(json -> writeYaml(json, new OutputStreamWriter(System.out)));
  }
}
