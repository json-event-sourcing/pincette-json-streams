# Parameters

Sometimes you will have stuff that you don't want to repeat all the time. With the top-level `parameters` object you can add values that are available in all parts of the application. Any JSON value is allowed. It works with simple substitution. In the following example the first parameter is an object. The second and third parameters have a value that starts with the `config:` prefix. This is how you can pull in values from the external configuration. A special parameter is `ENV`, which is drawn from the configuration entry `environment`.

If the parameter doesn't exist then the result will be an empty string.

Parameter references can be embedded in a string value of a field. However, this only works when the value of the parameter is a string. Otherwise the parameter reference should constitute the complete value of the field.

For string replacements you can use references such as `${<prefix>:NAME:<suffix>}`. The result is a string of the form `<prefix><replaced value><suffix>`. Prefixes and suffixes are optional. When the parameter doesn't exist, the whole reference is replaced with an empty string. The example below shows this with the `ENV` parameter.

Parameter values are allowed to have parameter references to other parameters.

If a string value in the configuration is actually a piece of JSON, then you can pull that in with the `config-json:` prefix. Wherever you refer to that parameter you will get a JSON value. You can also refer to a field inside of the parameter with a reference like `${PARAM.field}`.

```yaml
---
application: "my-app"
version: "1.0"
parameters:
  SETTINGS:
    delay: 10
    interval: 5
  ENDPOINT: "config:endpoint"
  CREDENTIALS: "config:credentials"
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

When application or pipeline parts are included through a filename reference, parameters can be overwritten or added if you use an `include` object instead of a direct filename. The object has the fields `file` and `parameters`.

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
  - include:
      file: "mypart.yml"
      parameters:
        SETTINGS:
          delay: 20
          interval: 5                
```