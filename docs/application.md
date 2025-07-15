# An Application

This is a declarative structure written in either JSON or YAML. YAML is more compact and makes it easier to add multi-line documentation. With the command-line interface you can convert from JSON to YAML and back.

The top-level of an application has at least the `application` and `parts` fields. The application name should be unique within the cluster. The parts form the structure of the application. Optionally the fields `version`, `parameters`, `title` and `description` can be added. All are strings.

This is a simple application with just one stream. It consumes the Kafka topic `my-commands` and retains only those messages that have the `_command` field set to `put`. The remaining messages are written to the Kafka topic `my-commands-filtered`.

```yaml
---
application: my-app
version: "1.0"
parts:
  - type: stream
    name: my-stream
    fromTopic: my-commands
    toTopic: my-commands-filtered
    pipeline:
      - $match:
          _command: put
```        

We could have left out the `toTopic` field. In that case the results are available as the stream called `my-stream`. Another part of the application can consume that stream by referring to its name. It could look like this:

```yaml
---
application: my-app
version: "1.0"
parts:
  - type: stream
    name: my-stream
    fromTopic: my-commands
    pipeline:
      - $match:
          _command: put
  - type: stream
    name: set-filtered
    fromStream: my-stream
    toTopic: my-commands-filtered
    pipeline:
      - $addFields:
          filtered: true    
```        

Now the second part takes the output of the first one and adds the `filtered` field to each message before writing it to the topic `my-commands-filtered`. Of course, in practice you would add the `$addFields` pipeline stage to the first part, right after the `$match` stage.

Sometimes a file can grow very large when there are many parts. Then it is more convenient to move the parts in separate files like this:

```yaml
---
application: my-app
version: "1.0"
parts:
  - my_stream.yml
  - set_filtered.yml
```

The files would then contain the following respectively:

```yaml
---
type: stream
name: my-stream
fromTopic: my-commands
pipeline:
  - $match:
      _command: put

---
type: stream
name: set-filtered
fromStream: my-stream
toTopic: my-commands-filtered
pipeline:
  - $addFields:
      filtered: true    
```        

Part files can also contain arrays of parts. Everything will be loaded and assembled into one array of parts. Several applications can be combined in one array as well. Each entry will still be treated as a separate application. This is an example:

```yaml
---
- application: my-app1
  version: "1.0"
  parts:
    - parts1.yml
- application: my-app2
  version: "1.0"
  parts:
    - parts2.yml  
```