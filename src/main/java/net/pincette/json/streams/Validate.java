package net.pincette.json.streams;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static net.pincette.json.JsonUtil.getObject;
import static net.pincette.json.JsonUtil.getObjects;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getStrings;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.streams.Common.AGGREGATE;
import static net.pincette.json.streams.Common.AGGREGATE_TYPE;
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
import static net.pincette.json.streams.Common.ON;
import static net.pincette.json.streams.Common.PARTS;
import static net.pincette.json.streams.Common.REDUCER;
import static net.pincette.json.streams.Common.RIGHT;
import static net.pincette.json.streams.Common.STREAM;
import static net.pincette.json.streams.Common.STREAM_TYPES;
import static net.pincette.json.streams.Common.TYPE;
import static net.pincette.json.streams.Common.VALIDATOR;
import static net.pincette.json.streams.Common.VERSION_FIELD;
import static net.pincette.json.streams.Common.application;
import static net.pincette.json.streams.Common.getCommands;
import static net.pincette.json.streams.Logging.severe;
import static net.pincette.util.Pair.pair;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.util.Pair;

class Validate {
  private Validate() {}

  private static String aggregateType(final JsonObject specification) {
    return specification.getString(AGGREGATE_TYPE, null);
  }

  private static Stream<Pair<String, String>> getStreamReferences(final JsonObject specification) {
    return concat(
            getObjects(specification, PARTS)
                .map(part -> pair(name(part), part.getString(FROM_STREAM, null))),
            getObjects(specification, PARTS)
                .flatMap(
                    part -> getStrings(part, FROM_STREAMS).map(stream -> pair(name(part), stream))))
        .filter(pair -> pair.first != null && pair.second != null);
  }

  private static Set<String> getStreams(final JsonObject specification) {
    return concat(getStreamsForStreamParts(specification), getStreamsForAggregates(specification))
        .collect(toSet());
  }

  private static Stream<String> getStreamsForAggregates(final JsonObject specification) {
    return getObjects(specification, PARTS)
        .filter(part -> AGGREGATE.equals(part.getString(TYPE, null)))
        .map(Validate::aggregateType)
        .filter(Objects::nonNull)
        .flatMap(
            type ->
                Stream.of("-aggregate", "-command", "-event", "-event-full", "-reply")
                    .map(kind -> type + kind));
  }

  private static Stream<String> getStreamsForStreamParts(final JsonObject specification) {
    return getObjects(specification, PARTS)
        .filter(part -> STREAM_TYPES.contains(part.getString(TYPE, "")))
        .map(Validate::name)
        .filter(Objects::nonNull);
  }

  private static String name(final JsonObject specification) {
    return specification.getString(NAME, null);
  }

  private static void report(final JsonObject specification, final Supplier<String> message) {
    severe(
        ofNullable(application(specification))
                .map(app -> "Application " + app + ": ")
                .orElse("Unknown application: ")
            + message.get());
  }

  private static String type(final JsonObject specification) {
    return specification.getString(TYPE, null);
  }

  private static boolean validateStreamReferences(final JsonObject specification) {
    final var streams = getStreams(specification);

    return getStreamReferences(specification)
        .filter(pair -> !streams.contains(pair.second))
        .map(
            pair ->
                SideEffect.<String>run(
                        () ->
                            report(
                                specification,
                                () ->
                                    "part "
                                        + pair.first
                                        + " refers to non-existing stream "
                                        + pair.second))
                    .andThenGet(() -> pair.first))
        .findAny()
        .isEmpty();
  }

  private static boolean validateAggregate(final JsonObject application, final JsonObject part) {
    var result =
        ofNullable(aggregateType(part)).filter(type -> type.indexOf('-') != -1).isPresent();

    if (!result) {
      report(
          application,
          () ->
              "aggregate "
                  + name(part)
                  + " should have the \"aggregateType\" field with the form <app>-<type>.");
    }

    result &= validateCommands(application, part);

    return result;
  }

  static boolean validateApplication(final JsonObject specification) {
    var result =
        application(specification) != null
            && getValue(specification, "/" + VERSION_FIELD).map(JsonUtil::isString).orElse(false)
            && getValue(specification, "/" + PARTS).map(JsonUtil::isArray).orElse(false);

    if (!result) {
      report(
          specification,
          () ->
              "a topology should have an \"application\" and a \"version\" field, both "
                  + "strings, and an array called \"parts\".");
    } else {
      result =
          validateStreamReferences(specification)
              && getObjects(specification, PARTS)
                  .allMatch(part -> validatePart(specification, part));
    }

    return result;
  }

  private static boolean validateCommand(
      final JsonObject application,
      final String aggregateType,
      final String name,
      final JsonObject command) {
    final var result =
        name != null
            && command.get(REDUCER) != null
            && (!command.containsKey(VALIDATOR) || getObject(command, "/" + VALIDATOR).isPresent());

    if (!result) {
      report(
          application,
          () ->
              "command "
                  + name
                  + " of aggregate "
                  + aggregateType
                  + " should have a \"reducer\" field and when the \"validator\" field is present"
                  + " it should be an object.");
    }

    return result;
  }

  private static boolean validateCommands(final JsonObject application, final JsonObject part) {
    return getCommands(part)
        .allMatch(
            pair -> validateCommand(application, aggregateType(part), pair.first, pair.second));
  }

  private static boolean validateJoin(final JsonObject application, final JsonObject part) {
    final var left = "/" + LEFT + "/";
    final var right = "/" + RIGHT + "/";
    final var result =
        name(part) != null
            && getValue(part, left + ON).isPresent()
            && getValue(part, right + ON).isPresent()
            && (getString(part, left + FROM_STREAM).isPresent()
                || getString(part, left + FROM_TOPIC).isPresent()
                || getString(part, left + FROM_COLLECTION).isPresent())
            && (getString(part, right + FROM_STREAM).isPresent()
                || getString(part, right + FROM_TOPIC).isPresent()
                || getString(part, right + FROM_COLLECTION).isPresent());

    if (!result) {
      report(
          application,
          () ->
              "the join "
                  + name(part)
                  + " should have the fields \"left.on\", \"right.on\", either "
                  + "\"left.fromCollection\", \"left.fromStream\" or \"left.fromTopic\" and "
                  + "either \"right.fromCollection\", \"right.fromStream\" or \"right.fromTopic\""
                  + ".");
    }

    return result;
  }

  private static boolean validateMerge(final JsonObject application, final JsonObject part) {
    final var result =
        name(part) != null
            && (part.getJsonArray(FROM_STREAMS) != null
                || part.getJsonArray(FROM_TOPICS) != null
                || part.getJsonArray(FROM_COLLECTIONS) != null);

    if (!result) {
      report(
          application,
          () ->
              "the merge "
                  + name(part)
                  + " should have the fields \"name\" and \"fromCollections\", \"fromStreams\""
                  + " or \"fromTopics\".");
    }

    return result;
  }

  private static boolean validatePart(final JsonObject application, final JsonObject part) {
    var result = type(part) != null;

    if (!result) {
      report(application, () -> "a part should have a \"type\" field.");
    }

    result = name(part) != null;

    if (!result) {
      report(application, () -> "a part should have a \"name\" field.");
    }

    return result
        && ofNullable(type(part)).filter(type -> validatePart(application, type, part)).isPresent();
  }

  private static boolean validatePart(
      final JsonObject application, final String type, final JsonObject part) {
    return switch (type) {
      case AGGREGATE -> validateAggregate(application, part);
      case JOIN -> validateJoin(application, part);
      case MERGE -> validateMerge(application, part);
      case STREAM -> validateStream(application, part);
      default -> false;
    };
  }

  private static boolean validateStream(final JsonObject application, final JsonObject part) {
    final var result =
        name(part) != null
            && (part.getString(FROM_STREAM, null) != null
                || part.getString(FROM_TOPIC, null) != null
                || part.getString(FROM_COLLECTION, null) != null);

    if (!result) {
      report(
          application,
          () ->
              "the stream "
                  + name(part)
                  + " should have the field \"name\" and either \"fromCollection\","
                  + " \"fromStream\" or \"fromTopic\".");
    }

    return result;
  }
}
