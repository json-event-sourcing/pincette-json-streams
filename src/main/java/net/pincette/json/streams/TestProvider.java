package net.pincette.json.streams;

import java.util.Set;
import java.util.concurrent.CompletionStage;

interface TestProvider<T, U, V, W> extends Provider<T, U, V, W> {
  CompletionStage<Boolean> allAssigned(final String application, final Set<String> topics);

  void createTopics(final Set<String> topics);

  void deleteConsumerGroups(final Set<String> groupIds);

  void deleteTopics(final Set<String> topics);

  Producer getProducer();
}
