package net.pincette.json.streams;

import static java.util.Optional.ofNullable;
import static java.util.logging.Level.INFO;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.tel.OtelLogger.log;
import static net.pincette.json.JsonUtil.asBoolean;
import static net.pincette.json.JsonUtil.asDouble;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isBoolean;
import static net.pincette.json.JsonUtil.isDouble;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.isStructure;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.streams.Common.LOG;
import static net.pincette.json.streams.Logging.logStageObject;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.util.ImmutableBuilder.create;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.streams.Stage;
import net.pincette.rs.streams.Message;
import net.pincette.util.ImmutableBuilder;

class LogStage {
  private static final String ATTRIBUTES = "attributes";
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

  private static Attributes getAttributes(
      final JsonObject json, final Function<JsonObject, JsonValue> attributes) {
    return attributes != null ? toAttributes(attributes.apply(json)) : null;
  }

  private static Level getLogLevel(
      final JsonObject json, final Function<JsonObject, JsonValue> level) {
    return callAsString(json, level).map(Level::parse).orElse(INFO);
  }

  static Stage logStage(final Context context) {
    return (expression, c) -> {
      if (!isObject(expression)) {
        logStageObject(LOG, expression);

        return passThrough();
      }

      final var expr = expression.asJsonObject();
      final var attributes =
          getValue(expr, "/" + ATTRIBUTES).map(a -> function(a, context.features)).orElse(null);
      final var level =
          getValue(expr, "/" + LEVEL).map(l -> function(l, context.features)).orElse(null);
      final var message =
          getValue(expr, "/" + MESSAGE).map(m -> function(m, context.features)).orElse(null);

      return message != null
          ? map(
              (Message<String, JsonObject> m) ->
                  SideEffect.<Message<String, JsonObject>>run(
                          () ->
                              log(
                                  context.logger.get(),
                                  getLogLevel(m.value, level),
                                  null,
                                  () -> messageValue(message.apply(m.value)),
                                  () -> getAttributes(m.value, attributes),
                                  traceId(m.value).orElse(null),
                                  rootSpanId(m.value).orElse(null)))
                      .andThenGet(() -> m))
          : passThrough();
    };
  }

  private static String messageValue(final JsonValue value) {
    return escapeFormatting(stringValue(value).orElseGet(() -> string(value)));
  }

  private static Optional<String> rootSpanId(final JsonObject json) {
    return traceId(json).map(s -> s.substring(0, 16));
  }

  private static ImmutableBuilder<AttributesBuilder> setAttribute(
      final ImmutableBuilder<AttributesBuilder> builder, final String key, final JsonValue value) {
    return builder
        .updateIf(b -> isStructure(value), b -> b.put(key, string(value)))
        .updateIf(b -> isBoolean(value), b -> b.put(key, asBoolean(value)))
        .updateIf(b -> isDouble(value), b -> b.put(key, asDouble(value)))
        .updateIf(b -> isString(value), b -> b.put(key, asString(value).getString()))
        .updateIf(b -> isLong(value), b -> b.put(key, asLong(value)));
  }

  private static Attributes toAttributes(final JsonValue value) {
    return ofNullable(value)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(
            o ->
                o.entrySet().stream()
                    .reduce(
                        create(Attributes::builder),
                        (b, e) -> setAttribute(b, e.getKey(), e.getValue()),
                        (b1, b2) -> b1)
                    .build()
                    .build())
        .orElse(null);
  }

  private static Optional<String> traceId(final JsonObject json) {
    return getString(json, "/" + CORR).map(s -> s.replace("-", ""));
  }
}
