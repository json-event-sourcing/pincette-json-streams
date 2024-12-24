# Plugins

If you want to add custom MongoDB operators, aggregate pipeline stages or JSLT functions in Java, then you can put them in a plugin. This should be a Java 9 module, which implements the interface [```net.pincette.json.streams.plugin.Plugin```](https://www.javadoc.io/doc/net.pincette/pincette-json-streams-plugin/latest/net.pincette.json.streams.plugin/net/pincette/json/streams/plugin/Plugin.html). Make sure that the modules your plugin depends on are in the same directory.

If the configuration has set the `plugins` entry to `/plugins`, for example, then all subdirectories of the latter will be loaded as plugins. Each subdirectory will have its own module layer. A plugin should have something like the following in its `module-info.java`:

```
requires net.pincette.json.streams.plugin;
provides net.pincette.json.streams.plugin.Plugin
      with be.lars.json.streams.plugin.Extensions;
```

In this case, the class `be.lars.json.streams.plugin.Extensions` implements the interface `net.pincette.json.streams.plugin.Plugin`.
