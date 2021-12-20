package net.pincette.json.streams;

import static java.lang.ClassLoader.getSystemResourceAsStream;
import static java.lang.System.getProperty;
import static java.util.logging.LogManager.getLogManager;
import static net.pincette.util.Collections.flatten;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.function.Function;
import java.util.logging.Logger;

class Logging {
  static final String LOGGER = "net.pincette.json.streams";
  private static final String LEVEL = ".level";
  private static final String LOG = "log";
  private static final String LOG_LEVEL = "logLevel";

  private Logging() {}

  private static void addLevels(final Properties logging, final Config config) {
    tryToGetSilent(() -> config.getObject(LOG))
        .map(ConfigObject::unwrapped)
        .map(map -> flatten(map, "."))
        .ifPresent(
            log ->
                log.entrySet().stream()
                    .map(e -> pair(e.getKey(), e.getValue()))
                    .filter(p -> p.second instanceof String)
                    .forEach(p -> logging.setProperty(p.first + LEVEL, (String) p.second)));
  }

  static void init(final Config config) {
    if (getProperty("java.util.logging.config.class") == null
        && getProperty("java.util.logging.config.file") == null) {
      final var logging = loadLogging();
      final var out = new ByteArrayOutputStream();

      tryToGetSilent(() -> config.getString(LOG_LEVEL))
          .ifPresent(level -> logging.setProperty(LEVEL, level));
      addLevels(logging, config);

      tryToDoRethrow(
          () -> {
            logging.store(out, null);
            getLogManager().readConfiguration(new ByteArrayInputStream(out.toByteArray()));
          });
    }
  }

  private static Properties loadLogging() {
    final var properties = new Properties();

    tryToDoRethrow(() -> properties.load(getSystemResourceAsStream("logging.properties")));

    return properties;
  }

  static <T> T trace(final String message, final T value, final String logger) {
    return trace(message, value, T::toString, logger);
  }

  static <T> T trace(
      final String message,
      final T value,
      final Function<T, String> createString,
      final String logger) {
    Logger.getLogger(logger)
        .finest(() -> logger + ": " + message + ": " + createString.apply(value));

    return value;
  }
}
