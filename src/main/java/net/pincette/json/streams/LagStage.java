package net.pincette.json.streams;

import static java.lang.String.valueOf;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.streams.Common.LAG;
import static net.pincette.json.streams.Logging.logStageObject;
import static net.pincette.mongo.Expression.function;
import static net.pincette.rs.Async.mapAsync;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.PassThrough.passThrough;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.mongo.streams.Stage;
import net.pincette.rs.streams.Message;
import net.pincette.util.State;

class LagStage {
  private static final String AS = "as";
  private static final Duration INTERVAL = ofSeconds(30);

  private LagStage() {}

  private static Predicate<Message<String, JsonObject>> filterMessages() {
    final State<Instant> last = new State<>();

    return m -> {
      if (last.get() == null || now().isAfter(last.get().plus(INTERVAL))) {
        last.set(now());

        return true;
      }

      return false;
    };
  }

  static Stage lagStage(
      final Supplier<CompletionStage<Map<String, Map<Partition, Long>>>> messageLag,
      final Context context) {
    return (expression, c) -> {
      if (!isObject(expression) || !expression.asJsonObject().containsKey(AS)) {
        logStageObject(LAG, expression);

        return passThrough();
      }

      final JsonObject expr = expression.asJsonObject();
      final Function<JsonObject, JsonValue> as =
          function(expr.getValue("/" + AS), context.features);

      return box(
          filter(filterMessages()),
          mapAsync(
              m ->
                  stringValue(as.apply(m.value))
                      .map(
                          a ->
                              messageLag
                                  .get()
                                  .thenApply(LagStage::toJson)
                                  .thenApply(
                                      json ->
                                          m.withValue(
                                              createObjectBuilder(m.value).add(a, json).build())))
                      .orElseGet(() -> completedFuture(m))));
    };
  }

  private static JsonObject toJson(final Map<String, Map<Partition, Long>> messageLags) {
    return messageLags.entrySet().stream()
        .reduce(
            createObjectBuilder(),
            (b, e) -> b.add(e.getKey(), toJsonPerPartition(e.getValue())),
            (b1, b2) -> b1)
        .build();
  }

  private static JsonObject toJsonPerPartition(final Map<Partition, Long> messageLags) {
    return from(
        messageLags.entrySet().stream()
            .collect(
                groupingBy(
                    e -> e.getKey().topic,
                    toMap(e -> valueOf(e.getKey().offset), Entry::getValue))));
  }
}
