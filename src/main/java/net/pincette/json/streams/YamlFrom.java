package net.pincette.json.streams;

import static net.pincette.json.JsonOrYaml.readYaml;
import static net.pincette.json.JsonOrYaml.writeJson;
import static net.pincette.json.streams.Application.APP_VERSION;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "from",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Convert YAML to JSON. It reads from stdin and writes to stdout.")
@SuppressWarnings("java:S106") // Not logging.
class YamlFrom implements Runnable {
  public void run() {
    readYaml(new InputStreamReader(System.in))
        .ifPresent(json -> writeJson(json, new OutputStreamWriter(System.out)));
  }
}
