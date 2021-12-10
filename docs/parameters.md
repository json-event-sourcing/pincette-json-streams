# Parameters

Sometimes you will have stuff that you don't want to repeat all the time. With the top-level `parameters` object you can add values that are available in all parts of the application. Any JSON value is allowed. It works with simple substitution. In the following example the first parameter is an object. The second and third parameters have a value that starts with the `config:` prefix. This is how you can pull in values from the external configuration. A special parameter is `ENV`, which is drawn from the configuration entry `environment`.

If the parameter doesn't exist then the result will be an empty string.

For string replacements you can use references such as `${<prefix>:NAME:<suffix>}`. The result is a string of the form `<prefix><replaced value><suffix>`. Prefixes and suffixes are optional. When the parameter doesn't exist, the whole reference is replaced with an empty string. The example below shows this with the `ENV` parameter.

```yaml
---
application: "my-app"
version: "1.0"
parameters:
  - SETTINGS:
      delay: 10
      interval: 5
  - ENDPOINT: "config:endpoint"
  - CREDENTIALS: "config:credentials"
parts:
  - type: "stream"
    name: "my-stream"
    fromTopic: "my-commands${-:ENV}"
    toTopic: "my-commands-filtered${-:ENV}"
    pipeline:
      - $match:
          _command: "put"
      - $http:
          url:
            $concat:
              - ${ENDPOINT}
              - "/resource"
          method: "POST"
          headers:
            Authorization:
              $concat:
                - "Basic "
                - "${CREDENTIALS}"
          body:
            command: "$$ROOT"
          as: "result"       
```