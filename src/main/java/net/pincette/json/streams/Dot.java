package net.pincette.json.streams;

import static java.util.Objects.hash;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
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
import static net.pincette.json.streams.Common.FROM_COLLECTION;
import static net.pincette.json.streams.Common.FROM_COLLECTIONS;
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
import static net.pincette.json.streams.Common.TO_COLLECTION;
import static net.pincette.json.streams.Common.TO_TOPIC;
import static net.pincette.json.streams.Common.TYPE;
import static net.pincette.json.streams.Common.application;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.concat;
import static net.pincette.util.Util.tryToDoWithRethrow;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
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
      description = "Create a graph with the applications.")
  private boolean global;

  Dot(final Supplier<Context> contextSupplier) {
    super(contextSupplier);
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

  private static Stream<Link> applicationLinks(final Stream<Link> links) {
    final List<Link> collected = links.collect(toList());

    return applicationLinks(
        collected.stream()
            .filter(l -> l.from.application != null)
            .collect(groupingBy(l -> l.from.application, mapping(l -> l.to, toSet()))),
        collected.stream()
            .filter(l -> l.to.application != null)
            .collect(groupingBy(l -> l.from, mapping(l -> l.to.application, toSet()))));
  }

  private static Stream<Link> applicationLinks(
      final Map<String, Set<End>> from, final Map<End, Set<String>> to) {
    return from.entrySet().stream()
        .flatMap(e -> e.getValue().stream().map(end -> pair(e.getKey(), end)))
        .flatMap(
            p ->
                ofNullable(to.get(p.second)).stream()
                    .flatMap(Set::stream)
                    .map(app -> pair(p.first, app)))
        .filter(p -> !p.first.equals(p.second))
        .map(p -> new Link(End.application(p.first), End.application(p.second)));
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
                    getString(json, "/" + LEFT + "/" + FROM_TOPIC).orElse(null),
                    getString(json, "/" + LEFT + "/" + FROM_COLLECTION).orElse(null)),
                End.stream(json.getString(NAME))),
            new Link(
                new End(
                    null,
                    getString(json, "/" + RIGHT + "/" + FROM_STREAM).orElse(null),
                    getString(json, "/" + RIGHT + "/" + FROM_TOPIC).orElse(null),
                    getString(json, "/" + RIGHT + "/" + FROM_COLLECTION).orElse(null)),
                End.stream(json.getString(NAME)))),
        toTopic(json),
        toCollection(json));
  }

  private static Stream<Link> joinLinks(final String application, final JsonObject json) {
    return concat(
        toTopic(application, json, "/" + LEFT + "/" + FROM_TOPIC),
        toTopic(application, json, "/" + RIGHT + "/" + FROM_TOPIC));
  }

  private static Stream<Link> mergeLinks(final JsonObject json) {
    final End thisEnd = End.stream(json.getString(NAME));

    return concat(
        getStrings(json, FROM_STREAMS).map(stream -> new Link(End.stream(stream), thisEnd)),
        getStrings(json, FROM_TOPICS).map(topic -> new Link(End.topic(topic), thisEnd)),
        getStrings(json, FROM_COLLECTIONS)
            .map(collection -> new Link(End.collection(collection), thisEnd)),
        toTopic(json),
        toCollection(json));
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
    printPart(name, " [shape=ellipse][href=\"" + name + ".svg\"][label=\"", writer);
  }

  private static void printCollection(final String name, final PrintWriter writer) {
    printPart(name, " [shape=cylinder][label=\"Topic ", writer);
  }

  private static void printEnd(final End end, final PrintWriter writer) {
    if (end.application != null) {
      printApplication(end.name(), writer);
    } else if (end.stream != null) {
      printStream(end.stream, writer);
    } else if (end.topic != null) {
      printTopic(end.topic, writer);
    } else {
      printCollection(end.collection, writer);
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
                new End(
                    null,
                    json.getString(FROM_STREAM, null),
                    json.getString(FROM_TOPIC, null),
                    json.getString(FROM_COLLECTION, null)),
                End.stream(json.getString(NAME)))),
        toTopic(json),
        toCollection(json));
  }

  private static Stream<Link> streamLinks(final String application, final JsonObject json) {
    return concat(fromTopic(application, json), toTopic(application, json, "/" + TO_TOPIC));
  }

  private static Stream<Link> toCollection(final JsonObject json) {
    return ofNullable(json.getString(TO_COLLECTION, null)).stream()
        .map(collection -> new Link(End.stream(json.getString(NAME)), End.collection(collection)));
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

  private void manyApplications(final Stream<JsonObject> specifications, final Context context) {
    tryToDoWithRethrow(
        () -> getWriter("applications.dot"),
        writer -> {
          writer.println("digraph Applications {");
          applicationLinks(
                  specifications.flatMap(
                      specification -> globalLinks(specification, context.environment)))
              .collect(toSet())
              .forEach(link -> printLink(link, writer));
          writer.println('}');
          writer.flush();
        });
  }

  private void oneApplication(final JsonObject specification, final Context context) {
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
    final var context = contextSupplier.get();

    if (global) {
      manyApplications(getValidatedApplications(context), context);
    } else {
      getValidatedApplications(context).forEach(s -> oneApplication(s, context));
    }
  }

  private static class End {
    private final String application;
    private final String collection;
    private final String stream;
    private final String topic;

    private End(
        final String application,
        final String stream,
        final String topic,
        final String collection) {
      this.application = application;
      this.stream = stream;
      this.topic = topic;
      this.collection = collection;
    }

    private static End application(final String application) {
      return new End(application, null, null, null);
    }

    private static End collection(final String collection) {
      return new End(null, null, null, collection);
    }

    private static End stream(final String stream) {
      return new End(null, stream, null, null);
    }

    private static End topic(final String topic) {
      return new End(null, null, topic, null);
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
                          && Objects.equals(topic, end.topic)
                          && Objects.equals(collection, end.collection))
              .orElse(false);
    }

    @Override
    public int hashCode() {
      return hash(application, stream, topic, collection);
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
