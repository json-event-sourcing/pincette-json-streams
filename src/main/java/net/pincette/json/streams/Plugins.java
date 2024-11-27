package net.pincette.json.streams;

import static java.util.Optional.ofNullable;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Plugins.loadPlugins;

import com.schibsted.spt.data.jslt.Function;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import net.pincette.json.streams.plugin.Plugin;
import net.pincette.mongo.Operator;
import net.pincette.mongo.QueryOperator;
import net.pincette.mongo.streams.Stage;

class Plugins {
  Map<String, Operator> expressionExtensions = new HashMap<>();
  Collection<Function> jsltFunctions = new ArrayList<>();
  Map<String, QueryOperator> matchExtensions = new HashMap<>();
  Map<String, Stage> stageExtensions = new HashMap<>();

  Plugins() {}

  static Plugins load(final Path directory) {
    final var plugins = new Plugins();

    plugins.mergePlugins(loadPlugins(directory, layer -> ServiceLoader.load(layer, Plugin.class)));

    return plugins;
  }

  private void mergePlugins(final Stream<Plugin> plugins) {
    plugins.forEach(
        p -> {
          ofNullable(p.expressionExtensions())
              .ifPresent(e -> expressionExtensions = merge(expressionExtensions, e));
          ofNullable(p.jsltFunctions()).ifPresent(e -> jsltFunctions.addAll(e));
          ofNullable(p.matchExtensions())
              .ifPresent(e -> matchExtensions = merge(matchExtensions, e));
          ofNullable(p.stageExtensions())
              .ifPresent(e -> stageExtensions = merge(stageExtensions, e));
        });
  }
}
