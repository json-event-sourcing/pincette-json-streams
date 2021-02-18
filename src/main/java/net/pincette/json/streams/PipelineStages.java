package net.pincette.json.streams;

import static java.util.Optional.ofNullable;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.ERROR;
import static net.pincette.jes.util.JsonFields.ERRORS;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.mongo.Expression.function;

import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.Validator;
import net.pincette.mongo.streams.Stage;

class PipelineStages {
  private static final String APPLICATION = "application";
  private static final String LEVEL = "level";
  private static final String MESSAGE = "message";

  private PipelineStages() {}

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
    return callAsString(json, application).map(Logger::getLogger).orElse(context.logger);
  }

  private static void logObject(
      final String name, final JsonValue expression, final Context context) {
    context.logger.log(
        SEVERE,
        "The value of {0} should be an object, but {1} was given.",
        new Object[] {name, string(expression)});
  }

  static Stage logStage(final Context context) {
    return (stream, expression, c) -> {
      if (!isObject(expression)) {
        logObject("$log", expression, context);

        return stream;
      }

      final JsonObject expr = expression.asJsonObject();
      final Function<JsonObject, JsonValue> application =
          getValue(expr, "/" + APPLICATION).map(a -> function(a, context.features)).orElse(null);
      final Function<JsonObject, JsonValue> level =
          getValue(expr, "/" + LEVEL).map(l -> function(l, context.features)).orElse(null);
      final Function<JsonObject, JsonValue> message =
          getValue(expr, "/" + MESSAGE).map(m -> function(m, context.features)).orElse(null);

      return message != null
          ? stream.mapValues(
              v ->
                  SideEffect.<JsonObject>run(
                          () ->
                              getLogger(v, application, context)
                                  .log(
                                      getLogLevel(v, level),
                                      escapeFormatting(string(message.apply(v))),
                                      traceId(v)))
                      .andThenGet(() -> v))
          : stream;
    };
  }

  private static Object[] traceId(final JsonObject json) {
    return getString(json, "/" + CORR)
        .map(corr -> new Object[] {"traceId=" + corr.toLowerCase()})
        .orElseGet(() -> new Object[] {});
  }

  static Stage validateStage(final Context context) {
    return (stream, expression, c) -> {
      if (!isObject(expression)) {
        logObject("$validate", expression, context);

        return stream;
      }

      final Function<JsonObject, JsonArray> validator =
          new Validator(c.features).validator(expression.asJsonObject());

      return stream.mapValues(
          v ->
              Optional.of(validator.apply(v))
                  .filter(errors -> !errors.isEmpty())
                  .map(
                      errors -> createObjectBuilder(v).add(ERROR, true).add(ERRORS, errors).build())
                  .orElse(v));
    };
  }
}
