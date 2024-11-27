package net.pincette.json.streams;

import static java.util.Optional.ofNullable;
import static java.util.regex.Pattern.compile;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.Transform.transform;
import static net.pincette.util.Util.replaceAll;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.json.Transform;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;

class Parameters {
  private static final Pattern ENV_PATTERN = compile("\\$\\{(\\w+)((\\.[\\w\\-]+)+)?}");
  private static final Pattern ENV_PATTERN_STRING = compile("\\$\\{([^:}]+:)?(\\w+)(:[^:}]+)?}");

  private Parameters() {}

  private static String extraPath(final String matched) {
    return ofNullable(matched)
        .map(path -> path.substring(1))
        .map(JsonUtil::toJsonPointer)
        .orElse("");
  }

  private static boolean hasConfigurationParameters(final JsonObject value) {
    return hasConfigurationParameters(value.values().stream());
  }

  private static boolean hasConfigurationParameters(final JsonArray value) {
    return hasConfigurationParameters(value.stream());
  }

  private static boolean hasConfigurationParameters(final Stream<JsonValue> values) {
    return values.anyMatch(Parameters::hasConfigurationParameters);
  }

  private static boolean hasConfigurationParameters(final JsonValue value) {
    final BooleanSupplier tryArray =
        () ->
            isArray(value)
                ? hasConfigurationParameters(value.asJsonArray())
                : stringValue(value).filter(Common::isConfigRef).isPresent();

    return isObject(value)
        ? hasConfigurationParameters(value.asJsonObject())
        : tryArray.getAsBoolean();
  }

  private static boolean hasParameter(final JsonValue value) {
    return isString(value) && hasParameter(asString(value).getString());
  }

  private static boolean hasParameter(final String s) {
    return ENV_PATTERN.matcher(s).find() || ENV_PATTERN_STRING.matcher(s).find();
  }

  private static String parameterPrefix(final String s) {
    return s != null ? s.substring(0, s.length() - 1) : "";
  }

  private static String parameterSuffix(final String s) {
    return s != null ? s.substring(1) : "";
  }

  static Transformer replaceParameters(final JsonObject parameters) {
    return Optional.of(resolve(parameters))
        .map(Parameters::replaceParametersTransformer)
        .orElseGet(Transform::nopTransformer);
  }

  private static Transformer replaceParametersTransformer(final JsonObject parameters) {
    return replaceParametersObject(parameters).thenApply(replaceParametersArray(parameters));
  }

  private static JsonValue replaceParameters(final String s, final JsonObject parameters) {
    return Optional.of(ENV_PATTERN.matcher(s))
        .filter(Matcher::matches)
        .map(
            matcher ->
                getValue(parameters, "/" + matcher.group(1))
                    .flatMap(
                        v ->
                            hasConfigurationParameters(v)
                                ? Optional.of(createValue(s))
                                : getValue(
                                    parameters,
                                    "/" + matcher.group(1) + extraPath(matcher.group(2))))
                    .orElseGet(() -> createValue("")))
        .orElseGet(() -> createValue(replaceParametersString(s, parameters)));
  }

  private static JsonArray replaceParameters(final JsonArray array, final JsonObject parameters) {
    return array.stream()
        .reduce(
            createArrayBuilder(),
            (b, v) ->
                b.add(hasParameter(v) ? replaceParameters(asString(v).getString(), parameters) : v),
            (b1, b2) -> b1)
        .build();
  }

  private static Transformer replaceParametersArray(final JsonObject parameters) {
    return new Transformer(
        e -> isArray(e.value),
        e ->
            Optional.of(
                new JsonEntry(e.path, replaceParameters(e.value.asJsonArray(), parameters))));
  }

  private static Transformer replaceParametersObject(final JsonObject parameters) {
    return new Transformer(
        e -> hasParameter(e.path) || hasParameter(e.value),
        e ->
            Optional.of(
                new JsonEntry(
                    hasParameter(e.path) ? replaceParametersString(e.path, parameters) : e.path,
                    hasParameter(e.value)
                        ? replaceParameters(asString(e.value).getString(), parameters)
                        : e.value)));
  }

  static String replaceParametersString(final String s, final JsonObject parameters) {
    return replaceAll(
        s,
        ENV_PATTERN_STRING,
        matcher ->
            ofNullable(parameters.get(matcher.group(2)))
                .map(value -> stringValue(value).orElseGet(() -> string(value)))
                .map(
                    value ->
                        parameterPrefix(matcher.group(1))
                            + value
                            + parameterSuffix(matcher.group(3)))
                .orElse(""));
  }

  static JsonObject resolve(final JsonObject parameters) {
    return transform(parameters, replaceParametersObject(parameters));
  }
}
