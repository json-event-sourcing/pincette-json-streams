package net.pincette.json.streams;

import static java.util.Optional.ofNullable;
import static java.util.logging.Level.INFO;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.streams.Common.LOG;
import static net.pincette.json.streams.Logging.LOGGER;
import static net.pincette.json.streams.Logging.logStageObject;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.PassThrough.passThrough;

import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.streams.Stage;
import net.pincette.rs.streams.Message;

class LogStage {
  private static final String APPLICATION = "application";
  private static final String LEVEL = "level";
  private static final String MESSAGE = "message";

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
      final JsonObject json, final Function<JsonObject, JsonValue> application) {
    return callAsString(json, application)
        .map(Logger::getLogger)
        .orElseGet(() -> Logger.getLogger(LOGGER));
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

      return message != null
          ? map(
              (Message<String, JsonObject> m) ->
                  SideEffect.<Message<String, JsonObject>>run(
                          () ->
                              getLogger(m.value, application)
                                  .log(
                                      getLogLevel(m.value, level),
                                      escapeFormatting(string(message.apply(m.value))),
                                      traceId(m.value)))
                      .andThenGet(() -> m))
          : passThrough();
    };
  }

  private static Object[] traceId(final JsonObject json) {
    return getString(json, "/" + CORR)
        .map(corr -> new Object[] {"traceId=" + corr.toLowerCase()})
        .orElseGet(() -> new Object[] {});
  }
}
