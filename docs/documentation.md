# Documentation

You can generate documentation for an application with the command-line command `doc`. It writes a markdown file to `stdout`, which contains the interlinked descriptions of the parts. At the application and part level the fields `title` and `description` are the sources of the documentation. For aggregate commands those fields are also used.

The `dot` command-line command generates a [dot file](https://www.graphviz.org) on `stdout`. It is a visual graph of how the Kafka topics and streams are connected. If you install the Graphviz command-line tool then generating a graph in SVG, for example, can be done like this:

```
> js dot -a <my-application> | dot -Tsvg > my-application.svg
```
