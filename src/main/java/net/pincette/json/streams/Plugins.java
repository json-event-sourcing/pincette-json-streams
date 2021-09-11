package net.pincette.json.streams;

import static java.lang.ModuleLayer.boot;
import static java.nio.file.Files.list;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static java.util.stream.StreamSupport.stream;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.schibsted.spt.data.jslt.Function;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.lang.module.ResolvedModule;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Stream;
import net.pincette.json.streams.plugin.Plugin;
import net.pincette.mongo.Operator;
import net.pincette.mongo.QueryOperator;
import net.pincette.mongo.streams.Stage;
import net.pincette.util.IsolatingClassLoader;

class Plugins {
  private static final String PLUGIN_SYSTEM_PACKAGE_PREFIXES = "pluginSystemPackagePrefixes";

  Map<String, Operator> expressionExtensions = new HashMap<>();
  Collection<Function> jsltFunctions = new ArrayList<>();
  Map<String, QueryOperator> matchExtensions = new HashMap<>();
  Map<String, Stage> stageExtensions = new HashMap<>();

  Plugins() {}

  private static Set<String> alreadyLoaded() {
    return boot().configuration().modules().stream().map(ResolvedModule::name).collect(toSet());
  }

  private static ModuleLayer createPluginLayer(
      final Path directory, final List<String> systemPackagePrefixes) {
    final var boot = boot();
    final var finder = new Finder(ModuleFinder.of(directory), alreadyLoaded());

    return boot.defineModulesWithOneLoader(
        boot.configuration().resolve(finder, ModuleFinder.of(), moduleNames(finder)),
        new IsolatingClassLoader(
            new String[0],
            concat(
                    Arrays.stream(
                        new String[] {
                          "org.apache.kafka", "io.jsonwebtoken", "org.slf4j", "io.netty"
                        }),
                    systemPackagePrefixes.stream())
                .toArray(String[]::new),
            new String[0],
            null,
            directory.toFile().listFiles()));
  }

  static Plugins load(final Path directory, final Context context) {
    final var plugins = new Plugins();

    plugins.mergePlugins(
        loadPlugins(
            directory,
            tryToGetSilent(() -> context.config.getStringList(PLUGIN_SYSTEM_PACKAGE_PREFIXES))
                .orElseGet(Collections::emptyList)));

    return plugins;
  }

  private static Stream<Plugin> loadPlugins(
      final Path directory, final List<String> systemPackagePrefixes) {
    return Optional.of(directory)
        .filter(Files::isDirectory)
        .flatMap(d -> tryToGetRethrow(() -> list(d)))
        .map(
            children ->
                children
                    .map(path -> createPluginLayer(path, systemPackagePrefixes))
                    .flatMap(
                        layer ->
                            stream(ServiceLoader.load(layer, Plugin.class).spliterator(), false)))
        .orElseGet(Stream::empty);
  }

  private static Set<String> moduleNames(final ModuleFinder finder) {
    return finder.findAll().stream().map(ref -> ref.descriptor().name()).collect(toSet());
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

  private static class Finder implements ModuleFinder {
    private final Set<String> alreadyLoaded;
    private final ModuleFinder delegate;

    private Finder(final ModuleFinder delegate, final Set<String> alreadyLoaded) {
      this.delegate = delegate;
      this.alreadyLoaded = alreadyLoaded;
    }

    public Optional<ModuleReference> find(final String name) {
      return Optional.of(name).filter(n -> !alreadyLoaded.contains(n)).flatMap(delegate::find);
    }

    public Set<ModuleReference> findAll() {
      return delegate.findAll().stream()
          .filter(r -> !alreadyLoaded.contains(r.descriptor().name()))
          .collect(toSet());
    }
  }
}
