package net.pincette.json.streams;

import static java.util.Optional.ofNullable;

record Partition(String topic, int offset) {
  @Override
  public boolean equals(final Object o) {
    return this == o
        || ofNullable(o)
            .filter(Partition.class::isInstance)
            .map(Partition.class::cast)
            .filter(p -> offset == p.offset && topic.equals(p.topic))
            .isPresent();
  }
}
