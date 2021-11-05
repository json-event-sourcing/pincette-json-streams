package net.pincette.json.streams;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.ofNullable;
import static net.pincette.json.JsonUtil.getObjects;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getStrings;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Common.AGGREGATE;
import static net.pincette.json.streams.Common.AGGREGATE_TYPE;
import static net.pincette.json.streams.Common.COMMAND;
import static net.pincette.json.streams.Common.EVENT;
import static net.pincette.json.streams.Common.EVENT_FULL;
import static net.pincette.json.streams.Common.FROM_STREAM;
import static net.pincette.json.streams.Common.FROM_STREAMS;
import static net.pincette.json.streams.Common.FROM_TOPIC;
import static net.pincette.json.streams.Common.FROM_TOPICS;
import static net.pincette.json.streams.Common.JOIN;
import static net.pincette.json.streams.Common.LEFT;
import static net.pincette.json.streams.Common.MERGE;
import static net.pincette.json.streams.Common.NAME;
import static net.pincette.json.streams.Common.PARTS;
import static net.pincette.json.streams.Common.REPLY;
import static net.pincette.json.streams.Common.RIGHT;
import static net.pincette.json.streams.Common.STREAM;
import static net.pincette.json.streams.Common.TO_TOPIC;
import static net.pincette.json.streams.Common.TYPE;
import static net.pincette.util.StreamUtil.concat;
import static net.pincette.util.Util.tryToDoWithRethrow;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.stream.Stream;
import javax.json.JsonObject;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "dot",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description =
        "Generate a dot file from an application. It can be converted to an image with "
            + "the dot command-line tool.")
class Dot extends ApplicationCommand implements Runnable {
  Dot(final Context context) {
    super(context);
  }

  private static Stream<Link> aggregateLinks(final JsonObject json, final String environment) {
    return Stream.of(
        new Link(
            aggregateSubject(json, AGGREGATE),
            null,
            null,
            aggregateTopic(json, AGGREGATE, environment)),
        new Link(
            null,
            aggregateTopic(json, COMMAND, environment),
            aggregateSubject(json, COMMAND),
            null),
        new Link(
            aggregateSubject(json, EVENT), null, null, aggregateTopic(json, EVENT, environment)),
        new Link(
            aggregateSubject(json, EVENT_FULL),
            null,
            null,
            aggregateTopic(json, EVENT_FULL, environment)),
        new Link(
            aggregateSubject(json, REPLY), null, null, aggregateTopic(json, REPLY, environment)));
  }

  private static String aggregateSubject(final JsonObject json, final String subject) {
    return json.getString(AGGREGATE_TYPE) + "-" + subject;
  }

  private static String aggregateTopic(
      final JsonObject json, final String subject, final String environment) {
    return aggregateSubject(json, subject) + (environment != null ? ("-" + environment) : "");
  }

  private static Stream<Link> joinLinks(final JsonObject json) {
    return concat(
        Stream.of(
            new Link(
                getString(json, "/" + LEFT + "/" + FROM_STREAM).orElse(null),
                getString(json, "/" + LEFT + "/" + FROM_TOPIC).orElse(null),
                json.getString(NAME),
                null),
            new Link(
                getString(json, "/" + RIGHT + "/" + FROM_STREAM).orElse(null),
                getString(json, "/" + RIGHT + "/" + FROM_STREAM).orElse(null),
                json.getString(NAME),
                null)),
        toTopic(json));
  }

  private static Stream<Link> links(final JsonObject specification, final String environment) {
    return getObjects(specification, PARTS).flatMap(part -> partLinks(part, environment));
  }

  private static Stream<Link> mergeLinks(final JsonObject json) {
    return concat(
        concat(
            getStrings(json, FROM_STREAMS)
                .map(stream -> new Link(stream, null, json.getString(NAME), null)),
            getStrings(json, FROM_TOPICS)
                .map(topic -> new Link(null, topic, json.getString(NAME), null))),
        toTopic(json));
  }

  private static Stream<Link> partLinks(final JsonObject json, final String environment) {
    switch (json.getString(TYPE)) {
      case AGGREGATE:
        return aggregateLinks(json, environment);
      case JOIN:
        return joinLinks(json);
      case MERGE:
        return mergeLinks(json);
      case STREAM:
        return streamLinks(json);
      default:
        return Stream.empty();
    }
  }

  private static void printLink(final Link link, final PrintWriter writer) {
    printFromPart(link, writer);
    printToPart(link, writer);
    printName(link.from(), writer);
    writer.print(" -> ");
    printName(link.to(), writer);
    writer.println(';');
  }

  private static void printFromPart(final Link link, final PrintWriter writer) {
    if (link.fromStream != null) {
      printStream(link.fromStream, writer);
    } else {
      printTopic(link.fromTopic, writer);
    }
  }

  private static void printName(final String name, final PrintWriter writer) {
    writer.print('"');
    writer.print(name);
    writer.print('"');
  }

  private static void printPart(
      final String name, final String annotation, final PrintWriter writer) {
    printName(name, writer);
    writer.print(annotation);
    writer.print(name);
    writer.println("\"];");
  }

  private static void printStream(final String name, final PrintWriter writer) {
    printPart(name, " [shape=ellipse][label=\"Stream ", writer);
  }

  private static void printToPart(final Link link, final PrintWriter writer) {
    if (link.toStream != null) {
      printStream(link.toStream, writer);
    } else {
      printTopic(link.toTopic, writer);
    }
  }

  private static void printTopic(final String name, final PrintWriter writer) {
    printPart(name, " [shape=box][label=\"Topic ", writer);
  }

  private static Stream<Link> streamLinks(final JsonObject json) {
    return concat(
        Stream.of(
            new Link(
                json.getString(FROM_STREAM, null),
                json.getString(FROM_TOPIC, null),
                json.getString(NAME),
                null)),
        toTopic(json));
  }

  private static Stream<Link> toTopic(final JsonObject json) {
    return ofNullable(json.getString(TO_TOPIC, null)).stream()
        .map(topic -> new Link(json.getString(NAME), null, null, topic));
  }

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    tryToDoWithRethrow(
        () -> new PrintWriter(new OutputStreamWriter(System.out, UTF_8)),
        writer ->
            getValidatedTopology()
                .map(specification -> links(specification, context.environment))
                .ifPresent(
                    links -> {
                      writer.println("digraph Application {");
                      links.forEach(link -> printLink(link, writer));
                      writer.println('}');
                    }));
  }

  private static class Link {
    private final String fromStream;
    private final String fromTopic;
    private final String toStream;
    private final String toTopic;

    private Link(
        final String fromStream,
        final String fromTopic,
        final String toStream,
        final String toTopic) {
      this.fromStream = fromStream;
      this.fromTopic = fromTopic;
      this.toStream = toStream;
      this.toTopic = toTopic;
    }

    private String from() {
      return fromStream != null ? fromStream : fromTopic;
    }

    private String to() {
      return toStream != null ? toStream : toTopic;
    }
  }
}
