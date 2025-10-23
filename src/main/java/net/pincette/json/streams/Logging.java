package net.pincette.json.streams;

import static java.util.Optional.ofNullable;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.parse;
import static java.util.logging.LogManager.getLogManager;
import static net.pincette.config.Util.configValue;
import static net.pincette.jes.tel.OtelUtil.logRecordProcessor;
import static net.pincette.jes.tel.OtelUtil.otelLogHandler;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.streams.Application.APP_VERSION;
import static net.pincette.json.streams.Common.namespace;
import static net.pincette.util.Collections.flatten;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.stream;
import static net.pincette.util.Util.initLogging;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.opentelemetry.sdk.logs.LogRecordProcessor;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonValue;

class Logging {
  static final String LOGGER_NAME = "net.pincette.json.streams";
  private static final Map<String, Logger> LOGGERS = new HashMap<>();
  static final Logger LOGGER = getLogger(LOGGER_NAME);
  private static final String LEVEL = ".level";
  private static final String LOG = "log";
  private static final String LOG_LEVEL = "logLevel";

  private Logging() {}

  private static void addLevels(final Config config) {
    tryToGetSilent(() -> config.getObject(LOG))
        .map(ConfigObject::unwrapped)
        .map(map -> flatten(map, "."))
        .ifPresent(
            log ->
                log.entrySet().stream()
                    .map(e -> pair(e.getKey(), e.getValue().toString()))
                    .flatMap(
                        pair ->
                            tryToGetSilent(() -> parse(pair.second))
                                .map(level -> pair(pair.first, level))
                                .stream())
                    .forEach(pair -> getLogger(pair.first).setLevel(pair.second)));
  }

  private static void addOtelHandlers(final Stream<String> loggers, final Config config) {
    logRecordProcessor(config).ifPresent(p -> addOtelHandlers(p, loggers, namespace(config)));
  }

  private static void addOtelHandlers(
      final LogRecordProcessor processor, final Stream<String> loggers, final String namespace) {
    loggers
        .filter(name -> !name.isEmpty())
        .flatMap(
            name ->
                otelLogHandler(namespace, name, APP_VERSION, processor)
                    .map(h -> pair(name, h))
                    .stream())
        .forEach(pair -> getLogger(pair.first).addHandler(pair.second));
  }

  static void exception(final Throwable e) {
    LOGGER.log(SEVERE, e.getMessage(), e);
  }

  static void exception(final Throwable e, final Supplier<String> message) {
    exception(e, message, () -> LOGGER);
  }

  static void exception(
      final Throwable e, final Supplier<String> message, final Supplier<Logger> logger) {
    logger
        .get()
        .log(
            SEVERE,
            e,
            () -> ofNullable(message).map(Supplier::get).map(m -> (m + "\n")).orElse(""));
  }

  static void finest(final String message) {
    LOGGER.finest(message);
  }

  static void finest(final Supplier<String> message) {
    LOGGER.finest(message);
  }

  static Logger getLogger(final String name) {
    return LOGGERS.computeIfAbsent(name, Logger::getLogger);
  }

  static void info(final String message) {
    LOGGER.info(message);
  }

  static void info(final Supplier<String> message) {
    LOGGER.info(message);
  }

  static void init(final Config config) {
    initLogging();
    setGlobalLogLevel(config);
    addLevels(config);
    addOtelHandlers(stream(getLogManager().getLoggerNames()), config);
  }

  static void logStageObject(final String name, final JsonValue expression) {
    LOGGER.log(
        SEVERE,
        "The value of {0} should be an object, but {1} was given.",
        new Object[] {name, string(expression)});
  }

  private static void setGlobalLogLevel(final Config config) {
    configValue(config::getString, LOG_LEVEL)
        .ifPresent(
            level ->
                tryToDoRethrow(
                    () ->
                        getLogManager()
                            .updateConfiguration(
                                property -> ((o, n) -> property.equals(LEVEL) ? level : o))));
  }

  static void severe(final String message) {
    LOGGER.severe(message);
  }

  static void severe(final Supplier<String> message) {
    LOGGER.severe(message);
  }

  static <T> T trace(final T value) {
    return trace(null, value, LOGGER);
  }

  static <T> T trace(final Supplier<String> message, final T value) {
    return trace(message, value, LOGGER);
  }

  static <T> T trace(final Supplier<String> message, final T value, final Logger logger) {
    return trace(message, value, v -> v != null ? v.toString() : "", logger);
  }

  static <T> T trace(
      final Supplier<String> message,
      final T value,
      final Function<T, String> createString,
      final Logger logger) {
    logger.finest(
        () ->
            logger.getName()
                + ": "
                + (message != null ? (message.get() + ": ") : "")
                + createString.apply(value));

    return value;
  }
}
