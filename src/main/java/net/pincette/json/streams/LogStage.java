package net.pincette.json.streams;

import static java.util.Optional.ofNullable;
import static java.util.logging.Level.INFO;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.streams.Common.LOG;
import static net.pincette.json.streams.Logging.logStageObject;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.util.Collections.expand;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.streams.Stage;
import net.pincette.rs.streams.Message;
import net.pincette.util.Collections;

class LogStage {
  private static final String APPLICATION = "application";
  private static final String ID = "id";
  private static final String LEVEL = "level";
  private static final String MESSAGE = "message";
  private static final Set<String> STANDARD = set(APPLICATION, LEVEL, MESSAGE);
  private static final String TRACE = "trace";
  private static final String TRACE_ID = TRACE + "." + ID;

  private LogStage() {}

  private static Optional<String> callAsString(
      final JsonObject json, final Function<JsonObject, JsonValue> fn) {
    return ofNullable(fn)
        .map(v -> v.apply(json))
        .filter(JsonUtil::isString)
        .map(JsonUtil::asString)
        .map(JsonString::getString);
  }

  private static String escapeFormatting(final String s) {
    return "'" + s + "'";
  }

  private static Level getLogLevel(
      final JsonObject json, final Function<JsonObject, JsonValue> level) {
    return callAsString(json, level).map(Level::parse).orElse(INFO);
  }

  private static Logger getLogger(
      final JsonObject json,
      final Function<JsonObject, JsonValue> application,
      final Context context) {
    return callAsString(json, application).map(Logging::getLogger).orElseGet(context.logger);
  }

  private static Object[] getParameters(
      final JsonObject json, final Map<String, Function<JsonObject, JsonValue>> functions) {
    return concat(
            functions.containsKey(TRACE) || functions.containsKey(TRACE_ID)
                ? Stream.empty()
                : traceId(json).stream(),
            functions.entrySet().stream()
                .map(e -> Collections.map(pair(e.getKey(), e.getValue().apply(json))))
                .map(map -> expand(map, "."))
                .map(JsonUtil::from))
        .toArray(Object[]::new);
  }

  static Stage logStage(final Context context) {
    return (expression, c) -> {
      if (!isObject(expression)) {
        logStageObject(LOG, expression);

        return passThrough();
      }

      final var expr = expression.asJsonObject();
      final var application =
          getValue(expr, "/" + APPLICATION).map(a -> function(a, context.features)).orElse(null);
      final var level =
          getValue(expr, "/" + LEVEL).map(l -> function(l, context.features)).orElse(null);
      final var message =
          getValue(expr, "/" + MESSAGE).map(m -> function(m, context.features)).orElse(null);
      final var parameters = parameterFunctions(expr, context);

      return message != null
          ? map(
              (Message<String, JsonObject> m) ->
                  SideEffect.<Message<String, JsonObject>>run(
                          () ->
                              getLogger(m.value, application, context)
                                  .log(
                                      getLogLevel(m.value, level),
                                      messageValue(message.apply(m.value)),
                                      getParameters(m.value, parameters)))
                      .andThenGet(() -> m))
          : passThrough();
    };
  }

  private static String messageValue(final JsonValue value) {
    return escapeFormatting(stringValue(value).orElseGet(() -> string(value)));
  }

  private static Map<String, Function<JsonObject, JsonValue>> parameterFunctions(
      final JsonObject expression, final Context context) {
    return expression.entrySet().stream()
        .filter(e -> !STANDARD.contains(e.getKey()))
        .collect(toMap(Entry::getKey, e -> function(e.getValue(), context.features)));
  }

  private static Optional<Map<String, Object>> traceId(final JsonObject json) {
    return getString(json, "/" + CORR)
        .map(corr -> Collections.map(pair(TRACE, Collections.map(pair(ID, corr.toLowerCase())))));
  }
}
