package net.pincette.json.streams;

import javax.json.JsonObject;

class Loaded {
  final String path;
  final JsonObject specification;

  Loaded(final JsonObject specification) {
    this(specification, null);
  }

  Loaded(final JsonObject specification, final String path) {
    this.specification = specification;
    this.path = path;
  }
}
