package net.pincette.json.streams;

import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static net.pincette.json.JsonUtil.getObjects;
import static net.pincette.json.JsonUtil.getStrings;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Common.AGGREGATE;
import static net.pincette.json.streams.Common.AGGREGATE_TYPE;
import static net.pincette.json.streams.Common.APPLICATION_FIELD;
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
import static net.pincette.json.streams.Common.VERSION_FIELD;
import static net.pincette.json.streams.Common.application;
import static net.pincette.json.streams.Common.getCommands;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.StreamUtil.concat;
import static net.pincette.util.Util.repeat;
import static net.pincette.util.Util.tryToDoWithRethrow;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.json.JsonObject;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "doc",
    version = APP_VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Generate a markdown document from an application.")
class Doc extends ApplicationCommand implements Runnable {
  private static final String DESCRIPTION = "description";
  private static final String TITLE = "title";

  Doc(final Supplier<Context> contextSupplier) {
    super(contextSupplier);
  }

  private static Function<JsonObject, Stream<String>> aggregateAnchors() {
    return json ->
        Optional.of(json.getString(AGGREGATE_TYPE)).stream()
            .flatMap(
                type ->
                    Stream.of(AGGREGATE, COMMAND, EVENT, EVENT_FULL, REPLY)
                        .map(kind -> type + "-" + kind));
  }

  private static Stream<String> aggregateCommands(final JsonObject json) {
    return concat(
        Stream.of(title("Commands", 3)),
        getCommands(json)
            .sorted(comparing(pair -> pair.first))
            .flatMap(pair -> command(pair.first, pair.second)));
  }

  private static Stream<String> aggregateMetadata(final JsonObject json) {
    return field(json, AGGREGATE_TYPE, "Aggregate type");
  }

  private static String anchors(final Stream<String> anchors) {
    return anchors.map(a -> "<span id=\"" + a + "\"/>").collect(joining());
  }

  private static Function<JsonObject, Stream<String>> anchors(final JsonObject json) {
    switch (json.getString(TYPE)) {
      case AGGREGATE:
        return aggregateAnchors();
      case JOIN:
      case MERGE:
      case STREAM:
        return streamAnchor();
      default:
        return noAnchor();
    }
  }

  private static Stream<String> applicationMetadata(final JsonObject json) {
    return concat(field(json, APPLICATION_FIELD, "Name"), field(json, VERSION_FIELD, "Version"));
  }

  private static String asLink(final String s) {
    return "[" + s + "](#" + s + ")";
  }

  private static Stream<String> blocks(final JsonObject specification) {
    return Stream.concat(
        section(specification, Doc::applicationMetadata, noAnchor(), 1), parts(specification));
  }

  private static Stream<String> command(final String name, final JsonObject command) {
    return concat(Stream.of(title(name, 4)), description(command));
  }

  private static Stream<String> description(final JsonObject json) {
    return ofNullable(json.getString(DESCRIPTION, null)).stream()
        .filter(s -> s.length() > 0)
        .map(s -> s.charAt(s.length() - 1) == '\n' ? s.substring(0, s.length() - 1) : s);
  }

  private static Stream<String> field(
      final JsonObject json, final String field, final String label) {
    return field(json, field, label, v -> v);
  }

  private static Stream<String> field(
      final JsonObject json,
      final String field,
      final String label,
      final UnaryOperator<String> decorator) {
    return ofNullable(json.getString(field, null)).stream()
        .map(f -> field(label, decorator.apply(f)));
  }

  private static String field(final String label, final String value) {
    return "*" + label + "*: " + value;
  }

  private static Stream<String> fieldArray(
      final JsonObject json, final String field, final String label) {
    return fieldArray(json, field, label, v -> v);
  }

  private static Stream<String> fieldArray(
      final JsonObject json,
      final String field,
      final String label,
      final UnaryOperator<String> decorator) {
    return Optional.of(getStrings(json, field).collect(joining(", ")))
        .filter(value -> value.length() > 0)
        .stream()
        .map(value -> field(label, decorator.apply(value)));
  }

  private static Stream<String> from(final JsonObject json) {
    return concat(
        field(json, FROM_STREAM, "From stream", Doc::asLink),
        field(json, FROM_TOPIC, "From topic"),
        field(json, FROM_COLLECTION, "From collection"));
  }

  private static Optional<String> getTitle(final JsonObject json) {
    return tryWith(() -> json.getString(TITLE, null))
        .or(() -> json.getString(NAME, null))
        .or(() -> application(json))
        .get();
  }

  private static Stream<String> joinMetadata(final JsonObject json) {
    return concat(
        Stream.of(title("Left", 4)),
        from(json.getJsonObject(LEFT)),
        Stream.of(title("Right", 4)),
        from(json.getJsonObject(RIGHT)),
        to(json));
  }

  private static Stream<String> mergeMetadata(final JsonObject json) {
    return concat(
        fieldArray(json, FROM_STREAMS, "From streams", Doc::asLink),
        fieldArray(json, FROM_TOPICS, "From topics"),
        fieldArray(json, FROM_COLLECTIONS, "From collections"));
  }

  private static Function<JsonObject, Stream<String>> metadata(final JsonObject json) {
    switch (json.getString(TYPE)) {
      case AGGREGATE:
        return Doc::aggregateMetadata;
      case JOIN:
        return Doc::joinMetadata;
      case MERGE:
        return Doc::mergeMetadata;
      case STREAM:
        return Doc::streamMetadata;
      default:
        return j -> Stream.empty();
    }
  }

  private static Function<JsonObject, Stream<String>> noAnchor() {
    return json -> Stream.empty();
  }

  private static Stream<String> part(final JsonObject json) {
    return concat(
        section(json, Doc::partMetadata, anchors(json), 2),
        Optional.of(json.getString(TYPE)).filter(type -> type.equals(AGGREGATE)).stream()
            .flatMap(type -> aggregateCommands(json)));
  }

  private static Stream<String> partMetadata(final JsonObject json) {
    return concat(field(json, NAME, "Name"), field(json, TYPE, "Type"), metadata(json).apply(json));
  }

  private static Stream<String> parts(final JsonObject specification) {
    return getObjects(specification, PARTS).flatMap(Doc::part);
  }

  private static Stream<String> section(
      final JsonObject json,
      final Function<JsonObject, Stream<String>> metadata,
      final Function<JsonObject, Stream<String>> anchors,
      final int level) {
    return concat(
        getTitle(json).stream().map(title -> title(title, level) + anchors(anchors.apply(json))),
        metadata.apply(json),
        description(json));
  }

  private static Function<JsonObject, Stream<String>> streamAnchor() {
    return json -> Stream.of(json.getString(NAME));
  }

  private static Stream<String> streamMetadata(final JsonObject json) {
    return concat(from(json), to(json));
  }

  private static String title(final String title, final int level) {
    return new String(repeat('#', level)) + " " + title;
  }

  private static Stream<String> to(final JsonObject json) {
    return concat(field(json, TO_TOPIC, "To topic"), field(json, TO_COLLECTION, "To collection"));
  }

  public void run() {
    getValidatedApplications(contextSupplier.get())
        .forEach(
            specification ->
                tryToDoWithRethrow(
                    () -> getWriter(application(specification) + ".md"),
                    writer -> {
                      blocks(specification)
                          .forEach(
                              block -> {
                                writer.println(block);
                                writer.println();
                              });
                      writer.flush();
                    }));
  }
}
