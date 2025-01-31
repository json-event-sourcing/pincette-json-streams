package net.pincette.json.streams;

import static java.nio.charset.StandardCharsets.UTF_8;
import static net.pincette.json.JsonOrYaml.read;
import static net.pincette.json.JsonOrYaml.readJson;
import static net.pincette.json.JsonOrYaml.readYaml;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.streams.Common.RESOURCE;
import static net.pincette.json.streams.Common.asStream;
import static net.pincette.json.streams.Common.baseDirectory;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetWithRethrow;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonValue;

class Read {
  private Read() {}

  private static InputStream getInputStream(final String path, final File baseDirectory) {
    return path.startsWith(RESOURCE)
        ? Application.class.getResourceAsStream(path.substring(RESOURCE.length()))
        : tryToGetRethrow(() -> new FileInputStream(new File(baseDirectory, path))).orElse(null);
  }

  static JsonObject readObject(final String path, final File baseDirectory) {
    return tryToGetWithRethrow(
            () -> new InputStreamReader(getInputStream(path, baseDirectory), UTF_8),
            reader ->
                (path.endsWith(".yml") || path.endsWith(".yaml")
                        ? readYaml(reader)
                        : readJson(reader))
                    .map(JsonValue::asJsonObject)
                    .orElse(null))
        .orElse(null);
  }

  static JsonObject readObject(final File file) {
    return read(file).map(JsonValue::asJsonObject).orElse(null);
  }

  static Stream<Loaded> readTopologies(final File file) {
    return read(file)
        .map(
            json ->
                asStream(json)
                    .filter(value -> isObject(value) || isString(value))
                    .map(
                        value ->
                            isObject(value)
                                ? new Loaded(value.asJsonObject(), baseDirectory(file, null))
                                : readTopology(asString(value).getString(), file)))
        .orElseGet(Stream::empty);
  }

  private static Loaded readTopology(final String path, final File file) {
    return new Loaded(
        readObject(path, baseDirectory(file, null)),
        !path.startsWith(RESOURCE) ? baseDirectory(file, path) : null);
  }
}
