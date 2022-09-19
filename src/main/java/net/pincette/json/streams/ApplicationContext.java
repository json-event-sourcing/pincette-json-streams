package net.pincette.json.streams;

import java.io.File;

class ApplicationContext {
  final String application;
  final File baseDirectory;
  final Context context;

  ApplicationContext() {
    this(null, null, null);
  }

  private ApplicationContext(
      final String application, final File baseDirectory, final Context context) {
    this.application = application;
    this.baseDirectory = baseDirectory;
    this.context = context;
  }

  ApplicationContext withApplication(final String application) {
    return new ApplicationContext(application, baseDirectory, context);
  }

  ApplicationContext withBaseDirectory(final File baseDirectory) {
    return new ApplicationContext(application, baseDirectory, context);
  }

  ApplicationContext withContext(final Context context) {
    return new ApplicationContext(application, baseDirectory, context);
  }
}
