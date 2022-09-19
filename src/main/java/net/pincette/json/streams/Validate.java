package net.pincette.json.streams;

import static java.util.Optional.ofNullable;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static net.pincette.json.JsonUtil.getNumber;
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
import static net.pincette.json.streams.Common.WINDOW;
import static net.pincette.json.streams.Common.application;
import static net.pincette.json.streams.Common.getCommands;
import static net.pincette.util.Pair.pair;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.util.Pair;

class Validate {
  private Validate() {}

  private static Stream<Pair<String, String>> getStreamReferences(final JsonObject specification) {
    return concat(
            getObjects(specification, PARTS)
                .map(part -> pair(part.getString(NAME, null), part.getString(FROM_STREAM, null))),
            getObjects(specification, PARTS)
                .flatMap(
                    part ->
                        getStrings(part, FROM_STREAMS)
                            .map(stream -> pair(part.getString(NAME, null), stream))))
        .filter(pair -> pair.first != null && pair.second != null);
  }

  private static Set<String> getStreams(final JsonObject specification) {
    return concat(getStreamsForStreamParts(specification), getStreamsForAggregates(specification))
        .collect(toSet());
  }

  private static Stream<String> getStreamsForAggregates(final JsonObject specification) {
    return getObjects(specification, PARTS)
        .filter(part -> AGGREGATE.equals(part.getString(TYPE, null)))
        .map(part -> part.getString(AGGREGATE_TYPE, null))
        .filter(Objects::nonNull)
        .flatMap(
            type ->
                Stream.of("-aggregate", "-command", "-event", "-event-full", "-reply")
                    .map(kind -> type + kind));
  }

  private static Stream<String> getStreamsForStreamParts(final JsonObject specification) {
    return getObjects(specification, PARTS)
        .filter(part -> STREAM_TYPES.contains(part.getString(TYPE, "")))
        .map(part -> part.getString(NAME, null))
        .filter(Objects::nonNull);
  }

  private static boolean validateStreamReferences(final JsonObject specification) {
    final var streams = getStreams(specification);

    return getStreamReferences(specification)
        .filter(pair -> !streams.contains(pair.second))
        .map(
            pair ->
                SideEffect.<String>run(
                        () ->
                            getGlobal()
                                .log(
                                    SEVERE,
                                    "Part {0} refers to non-existing stream {1}",
                                    new Object[] {pair.first, pair.second}))
                    .andThenGet(() -> pair.first))
        .findAny()
        .isEmpty();
  }

  private static boolean validateAggregate(final JsonObject specification) {
    var result =
        ofNullable(specification.getString(AGGREGATE_TYPE, null))
            .filter(type -> type.indexOf('-') != -1)
            .isPresent();

    if (!result) {
      getGlobal()
          .severe(
              "An aggregate should have the \"aggregateType\" field with the form "
                  + "<app>-<type>.");
    }

    result &= validateCommands(specification);

    return result;
  }

  static boolean validateApplication(final JsonObject specification) {
    var result =
        application(specification) != null
            && getValue(specification, "/" + VERSION_FIELD).map(JsonUtil::isString).orElse(false)
            && getValue(specification, "/" + PARTS).map(JsonUtil::isArray).orElse(false);

    if (!result) {
      getGlobal()
          .severe(
              "A topology should have an \"application\" and a \"version\" field, both "
                  + "strings, and an array called \"parts\".");
    } else {
      result =
          validateStreamReferences(specification)
              && getObjects(specification, PARTS).allMatch(Validate::validatePart);
    }

    return result;
  }

  private static boolean validateCommand(
      final String aggregateType, final String name, final JsonObject command) {
    final var result =
        name != null
            && command.getString(REDUCER, null) != null
            && (!command.containsKey(VALIDATOR) || getObject(command, "/" + VALIDATOR).isPresent());

    if (!result) {
      getGlobal()
          .log(
              SEVERE,
              "Command {0} of aggregate {1} should have a \"reducer\" field and "
                  + "when the \"validator\" field is present it should be an object.",
              new Object[] {name, aggregateType});
    }

    return result;
  }

  private static boolean validateCommands(final JsonObject specification) {
    final var aggregateType = specification.getString(AGGREGATE_TYPE, null);

    return getCommands(specification)
        .allMatch(pair -> validateCommand(aggregateType, pair.first, pair.second));
  }

  private static boolean validateJoin(final JsonObject specification) {
    final var left = "/" + LEFT + "/";
    final var right = "/" + RIGHT + "/";
    final var result =
        specification.getString(NAME, null) != null
            && getNumber(specification, "/" + WINDOW).isPresent()
            && getValue(specification, left + ON).isPresent()
            && getValue(specification, right + ON).isPresent()
            && (getString(specification, left + FROM_STREAM).isPresent()
                || getString(specification, left + FROM_TOPIC).isPresent()
                || getString(specification, left + FROM_COLLECTION).isPresent())
            && (getString(specification, right + FROM_STREAM).isPresent()
                || getString(specification, right + FROM_TOPIC).isPresent()
                || getString(specification, right + FROM_COLLECTION).isPresent());

    if (!result) {
      getGlobal()
          .log(
              SEVERE,
              "The join {0} should have the fields \"window\", \"left.on\", \"right.on\", either "
                  + "\"left.fromCollection\", \"left.fromStream\" or \"left.fromTopic\" and "
                  + "either \"right.fromCollection\", \"right.fromStream\" or \"right.fromTopic\".",
              new Object[] {specification.getString(NAME, null)});
    }

    return result;
  }

  private static boolean validateMerge(final JsonObject specification) {
    final var result =
        specification.getString(NAME, null) != null
            && (specification.getJsonArray(FROM_STREAMS) != null
                || specification.getJsonArray(FROM_TOPICS) != null
                || specification.getJsonArray(FROM_COLLECTIONS) != null);

    if (!result) {
      getGlobal()
          .log(
              SEVERE,
              "The merge {0} should have the fields \"name\" and "
                  + "\"fromCollections\", \"fromStreams\" or \"fromTopics\".",
              new Object[] {specification.getString(NAME, null)});
    }

    return result;
  }

  private static boolean validatePart(final JsonObject specification) {
    var result = specification.getString(TYPE, null) != null;

    if (!result) {
      getGlobal().severe("A part should have a \"type\" field.");
    }

    result = specification.getString(NAME, null) != null;

    if (!result) {
      getGlobal().severe("A part should have a \"name\" field.");
    }

    return result
        && ofNullable(specification.getString(TYPE, null))
            .filter(type -> validatePart(type, specification))
            .isPresent();
  }

  private static boolean validatePart(final String type, final JsonObject specification) {
    switch (type) {
      case AGGREGATE:
        return validateAggregate(specification);
      case JOIN:
        return validateJoin(specification);
      case MERGE:
        return validateMerge(specification);
      case STREAM:
        return validateStream(specification);
      default:
        return false;
    }
  }

  private static boolean validateStream(final JsonObject specification) {
    final var result =
        specification.getString(NAME, null) != null
            && (specification.getString(FROM_STREAM, null) != null
                || specification.getString(FROM_TOPIC, null) != null
                || specification.getString(FROM_COLLECTION, null) != null);

    if (!result) {
      getGlobal()
          .log(
              SEVERE,
              "The stream {0} should have the field \"name\" and either "
                  + "\"fromCollection\", \"fromStream\" or \"fromTopic\".",
              new Object[] {specification.getString(NAME, null)});
    }

    return result;
  }
}
