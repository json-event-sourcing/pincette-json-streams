package net.pincette.json.streams;

import static java.util.Objects.hash;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
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
import static net.pincette.json.streams.Common.application;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.StreamUtil.concat;
import static net.pincette.util.Util.tryToDoWithRethrow;

import java.io.PrintWriter;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import javax.json.JsonObject;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "dot",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description =
        "Generate a dot file from an application. It can be converted to an image with "
            + "the dot command-line tool.")
class Dot extends ApplicationCommand implements Runnable {
  @Option(
      names = {"-g", "--global"},
      description = "Create a graph with the applications and their topics.")
  private boolean global;

  Dot(final Context context) {
    super(context);
  }

  private static Stream<Link> aggregateLinks(final JsonObject json, final String environment) {
    return Stream.of(
        new Link(
            End.stream(aggregateSubject(json, AGGREGATE)),
            End.topic(aggregateTopic(json, AGGREGATE, environment))),
        new Link(
            End.topic(aggregateTopic(json, COMMAND, environment)),
            End.stream(aggregateSubject(json, COMMAND))),
        new Link(
            End.stream(aggregateSubject(json, EVENT)),
            End.topic(aggregateTopic(json, EVENT, environment))),
        new Link(
            End.stream(aggregateSubject(json, EVENT_FULL)),
            End.topic(aggregateTopic(json, EVENT_FULL, environment))),
        new Link(
            End.stream(aggregateSubject(json, REPLY)),
            End.topic(aggregateTopic(json, REPLY, environment))));
  }

  private static Stream<Link> aggregateLinks(
      final String application, final JsonObject json, final String environment) {
    final End appEnd = End.application(application);

    return Stream.of(
        new Link(appEnd, End.topic(aggregateTopic(json, AGGREGATE, environment))),
        new Link(End.topic(aggregateTopic(json, COMMAND, environment)), appEnd),
        new Link(appEnd, End.topic(aggregateTopic(json, EVENT, environment))),
        new Link(appEnd, End.topic(aggregateTopic(json, EVENT_FULL, environment))),
        new Link(appEnd, End.topic(aggregateTopic(json, REPLY, environment))));
  }

  private static String aggregateSubject(final JsonObject json, final String subject) {
    return json.getString(AGGREGATE_TYPE) + "-" + subject;
  }

  private static String aggregateTopic(
      final JsonObject json, final String subject, final String environment) {
    return aggregateSubject(json, subject) + (environment != null ? ("-" + environment) : "");
  }

  private static Stream<Link> applicationLinks(
      final JsonObject specification, final String environment) {
    return getObjects(specification, PARTS).flatMap(part -> partLinks(part, environment));
  }

  private static Stream<Link> fromTopic(final String application, final JsonObject json) {
    return ofNullable(json.getString(FROM_TOPIC, null)).stream()
        .map(topic -> new Link(End.topic(topic), End.application(application)));
  }

  private static Stream<Link> globalLinks(
      final JsonObject specification, final String environment) {
    final String application = application(specification);

    return getObjects(specification, PARTS)
        .flatMap(part -> partLinksGlobal(application, part, environment));
  }

  private static Stream<Link> joinLinks(final JsonObject json) {
    return concat(
        Stream.of(
            new Link(
                new End(
                    null,
                    getString(json, "/" + LEFT + "/" + FROM_STREAM).orElse(null),
                    getString(json, "/" + LEFT + "/" + FROM_TOPIC).orElse(null)),
                End.stream(json.getString(NAME))),
            new Link(
                new End(
                    null,
                    getString(json, "/" + RIGHT + "/" + FROM_STREAM).orElse(null),
                    getString(json, "/" + RIGHT + "/" + FROM_TOPIC).orElse(null)),
                End.stream(json.getString(NAME)))),
        toTopic(json));
  }

  private static Stream<Link> joinLinks(final String application, final JsonObject json) {
    return concat(
        toTopic(application, json, "/" + LEFT + "/" + FROM_TOPIC),
        toTopic(application, json, "/" + RIGHT + "/" + FROM_TOPIC));
  }

  private static Stream<Link> mergeLinks(final JsonObject json) {
    return concat(
        concat(
            getStrings(json, FROM_STREAMS)
                .map(stream -> new Link(End.stream(stream), End.stream(json.getString(NAME)))),
            getStrings(json, FROM_TOPICS)
                .map(topic -> new Link(End.topic(topic), End.stream(json.getString(NAME))))),
        toTopic(json));
  }

  private static Stream<Link> mergeLinks(final String application, final JsonObject json) {
    final End appEnd = End.application(application);

    return getStrings(json, FROM_TOPICS).map(topic -> new Link(End.topic(topic), appEnd));
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

  private static Stream<Link> partLinksGlobal(
      final String application, final JsonObject json, final String environment) {
    switch (json.getString(TYPE)) {
      case AGGREGATE:
        return aggregateLinks(application, json, environment);
      case JOIN:
        return joinLinks(application, json);
      case MERGE:
        return mergeLinks(application, json);
      case STREAM:
        return streamLinks(application, json);
      default:
        return Stream.empty();
    }
  }

  private static void printApplication(final String name, final PrintWriter writer) {
    printPart(name, " [shape=ellipse][href=\"" + name + ".svg\"][label=\"Application ", writer);
  }

  private static void printEnd(final End end, final PrintWriter writer) {
    if (end.application != null) {
      printApplication(end.name(), writer);
    } else if (end.stream != null) {
      printStream(end.stream, writer);
    } else {
      printTopic(end.topic, writer);
    }
  }

  private static void printLink(final Link link, final PrintWriter writer) {
    printEnd(link.from, writer);
    printEnd(link.to, writer);
    printName(link.from.name(), writer);
    writer.print(" -> ");
    printName(link.to.name(), writer);
    writer.println(';');
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

  private static void printTopic(final String name, final PrintWriter writer) {
    printPart(name, " [shape=box][label=\"Topic ", writer);
  }

  private static Stream<Link> streamLinks(final JsonObject json) {
    return concat(
        Stream.of(
            new Link(
                new End(null, json.getString(FROM_STREAM, null), json.getString(FROM_TOPIC, null)),
                End.stream(json.getString(NAME)))),
        toTopic(json));
  }

  private static Stream<Link> streamLinks(final String application, final JsonObject json) {
    return concat(fromTopic(application, json), toTopic(application, json, "/" + TO_TOPIC));
  }

  private static Stream<Link> toTopic(final JsonObject json) {
    return ofNullable(json.getString(TO_TOPIC, null)).stream()
        .map(topic -> new Link(End.stream(json.getString(NAME)), End.topic(topic)));
  }

  private static Stream<Link> toTopic(
      final String application, final JsonObject json, final String pointer) {
    return getString(json, pointer).stream()
        .map(topic -> new Link(End.application(application), End.topic(topic)));
  }

  private void manyApplications(final Stream<JsonObject> specifications) {
    tryToDoWithRethrow(
        () -> getWriter("applications.dot"),
        writer -> {
          writer.println("digraph Applications {");
          specifications
              .flatMap(specification -> globalLinks(specification, context.environment))
              .collect(toSet())
              .forEach(link -> printLink(link, writer));
          writer.println('}');
          writer.flush();
        });
  }

  private void oneApplication(final JsonObject specification) {
    tryToDoWithRethrow(
        () -> getWriter(application(specification) + ".dot"),
        writer -> {
          writer.println("digraph Application {");
          applicationLinks(specification, context.environment)
              .forEach(link -> printLink(link, writer));
          writer.println('}');
          writer.flush();
        });
  }

  public void run() {
    if (global) {
      manyApplications(getValidatedTopologies());
    } else {
      getValidatedTopologies().forEach(this::oneApplication);
    }
  }

  private static class End {
    private final String application;
    private final String stream;
    private final String topic;

    private End(final String application, final String stream, final String topic) {
      this.application = application;
      this.stream = stream;
      this.topic = topic;
    }

    private static End application(final String application) {
      return new End(application, null, null);
    }

    private static End stream(final String stream) {
      return new End(null, stream, null);
    }

    private static End topic(final String topic) {
      return new End(null, null, topic);
    }

    @Override
    public boolean equals(final Object other) {
      return this == other
          || Optional.of(other)
              .filter(End.class::isInstance)
              .map(End.class::cast)
              .map(
                  end ->
                      Objects.equals(application, end.application)
                          && Objects.equals(stream, end.stream)
                          && Objects.equals(topic, end.topic))
              .orElse(false);
    }

    @Override
    public int hashCode() {
      return hash(application, stream, topic);
    }

    private String name() {
      return tryWith(() -> application).or(() -> stream).or(() -> topic).get().orElse("");
    }
  }

  private static class Link {
    private final End from;
    private final End to;

    private Link(final End from, final End to) {
      this.from = from;
      this.to = to;
    }

    @Override
    public boolean equals(final Object other) {
      return this == other
          || Optional.of(other)
              .filter(Link.class::isInstance)
              .map(Link.class::cast)
              .map(link -> Objects.equals(from, link.from) && Objects.equals(to, link.to))
              .orElse(false);
    }

    @Override
    public int hashCode() {
      return hash(from, to);
    }
  }
}
