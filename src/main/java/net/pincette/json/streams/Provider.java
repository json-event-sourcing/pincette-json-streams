package net.pincette.json.streams;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import javax.json.JsonObject;
import net.pincette.rs.streams.Streams;

interface Provider<T, U, V, W> extends AutoCloseable {
  Streams<String, JsonObject, T, U> builder(final String application);

  /**
   * @return Topic/partition/lag
   */
  CompletionStage<Map<String, Map<Partition, Long>>> messageLag(
      final Predicate<String> includeGroup);

  CompletionStage<Map<Partition, Long>> offsets();

  Streams<String, String, V, W> stringBuilder(final String application);
}
