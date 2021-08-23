package net.pincette.json.streams;

import static net.pincette.json.streams.Application.APP_VERSION;

import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "yaml",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class, YamlFrom.class, YamlTo.class},
    description = "Convert JSON to YAML and back.")
class Yaml {}
