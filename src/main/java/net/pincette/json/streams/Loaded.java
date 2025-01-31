package net.pincette.json.streams;

import java.io.File;
import javax.json.JsonObject;

class Loaded {
  final File baseDirectory;
  final JsonObject specification;

  Loaded(final JsonObject specification) {
    this(specification, null);
  }

  Loaded(final JsonObject specification, final File baseDirectory) {
    this.specification = specification;
    this.baseDirectory = baseDirectory;
  }
}
