package net.pincette.json.streams;

import static java.util.Objects.hash;
import static java.util.Optional.ofNullable;

class Partition {
  final int offset;
  final String topic;

  Partition(final String topic, final int offset) {
    this.topic = topic;
    this.offset = offset;
  }

  @Override
  public boolean equals(final Object o) {
    return this == o
        || ofNullable(o)
            .filter(Partition.class::isInstance)
            .map(Partition.class::cast)
            .filter(p -> offset == p.offset && topic.equals(p.topic))
            .isPresent();
  }

  @Override
  public int hashCode() {
    return hash(offset, topic);
  }
}
