# State Management

State is managed in an aggregate. An aggregate can define commands, which may have effect. When they do, the change is recorded and published as an event.

You manage the state by writing reducers for commands. A reducer receives a command and the current state of an aggregate instance. Its task is to calculate the new state. Reducers are supposed to not have side effects. As such, reducers are merely JSON transformations.

Reducers are written in [JQ](https://github.com/eiiches/jackson-jq), [JSLT](https://github.com/schibsted/jslt) or as an aggregation pipeline. A JSLT script receives an object with the fields `command` and `state`. It should return the new state. Scripts are allowed to import other scripts. You should always use relative filenames.

A pipeline receives the same object. Its output becomes the new state. Since you can add your own pipeline stages through a plugin, a reducer could also be written in Java. This is an example of reducers as pipelines.

```yaml
---
application: app
version: "1.0"
parts:
  - type: aggregate
    aggregateType: plusminus-counter
    name: plusminus
    preprocessor: duplicates.yaml
    commands:
      plus:
        reducer:
          - $replaceRoot:
              newRoot: $state
          - $addFields:
              value:
                $add:
                  - $value
                  - 1
        validator: validators/validate_plus.yaml
      minus:
        reducer:
          - $replaceRoot:
              newRoot: $state
          - $addFields:
              value:
                $add:
                  - $value
                  - -1
        validator: validators/validate_minus.yaml
```

## Aggregate Parts

An aggregate application part has the following fields:

|Field|Mandatory|Description|
|---|---|---|
|aggregateType|Yes|The name of the aggregate. Usually is it composed as `<app>-<type>`.|
|commands|No|A JSON object where the keys are the command names. If no commands are given only the built-in commands `put`, `delete` and `patch` will be available.|
|environment|No|The environment for the aggregate. This will be used for Kafka topic suffixes.|
|preprocessor|No|A pipeline that pre-processes commands before they are reduced. With this you can avoid adding another public Kafka topic in front of the command topic.| 
|type|Yes|The value is always `aggregate`.|
|uniqueExpression|No|A MongoDB expression that is executed on aggregate instances. This expresses the uniqueness of aggregate instances based on some criterion. If you use this feature then you must make sure all commands also have the fields that constitute the unique expression.|

Commands have the following fields:

|Field|Mandatory|Description|
|---|---|---|
|preprocessor|No|A pipeline that pre-processes commands with this name before they are reduced. With this you can avoid adding another public Kafka topic in front of the command topic.| 
|reducer|Yes|It can be the filename of a JSLT script, which may be relative. It receives a JSON object with the fields `command` and `state`. The generated object will be used as the new state. It can also be a pipeline, which also receives JSON objects with the fields `command` and `state`. The output of the pipeline becomes the new state.|
|validator|No|A command [validator](validator.md). The value of the field must be a filename, which may be relative. If an expression wants to refer to the current state of an aggregate instance it can use the field `_state`.|

An aggregate creates the streams with the names `<app>-<type>-aggregate`, `<app>-<type>-command`, `<app>-<type>-event`, `<app>-<type>-event-full` and `<app>-<type>-reply`. They correspond to the Kafka topics of an aggregate. You can connect to those streams in other parts of the application.

## Aggregates

An aggregate is a JSON document, which can have any structure plus the following technical fields:

|Field|Mandatory|Description|
|---|---|---|
|\_corr|Yes|The correlation identifier that was used by the last command. It is usually a UUID. It is propagated the event if one is produced.|
|\_deleted|No|This boolean marks the aggregate instance as deleted. This is a logical deletion.|
|\_id|Yes|The identifier of the aggregate instance. It is usually a UUID.|
|\_jwt|No|The decoded JSON Web Token that was used by the last command. It is propagated the event if one is produced.|
|\_seq|Yes|A sequence number. This is the sequence number of the last event.|
|\_type|Yes|The aggregate type, which is composed as `<application>-<name>`.|

## Commands

A command is a JSON document, which has the following technical fields on top of whatever you put in it:

|Field|Mandatory|Description|
|---|---|---|
|\_command|Yes|The name of the command. A reducer is selected based on this name.|
|\_corr|Yes|A correlation identifier. It is propagated throughout the flow. This is usually a UUID.|
|\_error|No|This Boolean indicates there is a problem with the command. It is set by the validator that comes with the reducer.|
|\_id|Yes|The identifier of the aggregate instance. It is usually a UUID.|
|\_jwt|No|The decoded JSON Web Token. It is propagated throughout the flow.|
|\_languages|No|An array of [language tags](https://datatracker.ietf.org/doc/html/rfc1766) in the order of preference. When a validator or some other component wishes to send messages to the user, it can use the proper language for it.|
|\_seq|No|A sequence number. If this field is present then its value should be the same as that field in the aggregate instance. Otherwise the command is ignored.|
|\_type|Yes|The aggregate type, which is composed as `<application>-<name>`.|

There are three built-in commands called `put`, `patch` and `delete`. The `put` command replaces the entire contents of the aggregate instance. The `patch` command has the array field `_ops`, which is a [JSON patch](https://tools.ietf.org/html/rfc6902) that is applied to the instance. The `delete` command performs a logical deletion. It sets the field `_deleted` to `true`. The reducers for these commands can be replaced.

## Events

An event is a JSON document. It is generated when the old and the new states of the aggregate instance are different. When a reducer doesn't change the instance nothing will be generated. An event has the following technical fields:

|Field|Mandatory|Description|
|---|---|---|
|\_after|No|An optional field that carries the new state of the aggregate instance.|
|\_before|No|An optional field that carries the previous state of the aggregate instance. When this field and the previous one are present we speak of a "full event".|
|\_command|Yes|The name of the command that caused the event to be created.|
|\_corr|Yes|The correlation identifier that was used by the last command. It is usually a UUID. If you process the event you should always propagate this.|
|\_id|Yes|The identifier of the aggregate instance. It is usually a UUID.|
|\_jwt|No|The decoded JSON Web Token that was used by the last command. If you process the event you should always propagate this.|
|\_ops|Yes|An array of operations as described in [RFC 6902](https://tools.ietf.org/html/rfc6902). It describes how an aggregate instance has changed after the reduction by a command.|
|\_seq|Yes|A sequence number. There should not be holes in the sequence. This would indicate corruption of the event log.|
|\_timestamp|Yes|The timestamp of the event creation in epoch millis.|
|\_type|Yes|The aggregate type, which is composed as `<application>-<name>`.|

## The Kafka Topics

The external interface at runtime is a set op Kafka topics. Their names always have the form `<application>-<type>-<purpose>[-<environment>]`. The following topics are expected to exist (the names are the purpose):

|Name|Mandatory|Description|
|---|---|---|
|aggregate|Yes|On this topic the current state of the aggregate is emitted when it has changed.|
|command|Yes|Through this topic commands are received. It is the only input of an aggregate.|
|event|Yes|On this topic the events are emitted, which contain the changes between two subsequent aggregate versions.|
|event-full|Yes|The events are also emitted on this topic, but here they have two extra fields. The `_before` field contains the previous state of the aggregate instance, while `_after` contains the one after the reduction. This is for consumers that want to do other kinds of analysis than with the plain difference, which is in the `_ops` field.|
|reply|Yes|On this topic either the new aggregate states or the commands that didn't pass validation are emitted. The topic is meant to be routed back to the end-user, for example through [Server-sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html#server-sent-events).|
|unique|No|This topic is only required when a "unique" MongoDB expression is given to the aggregate object. Commands will be re-keyed on this topic using the key values generated by the expression.|

The number of topic partitions should be the same for all topics. This is the upper limit for the parallelism you can achieve for one aggregate. It is best to provide more partitions than the level of parallelism you want to start with. This allows you to scale out without having to extend the number of partitions, for which down time would be needed if you use Kafka. Note, however, that partitions don't come cheap.

## Storing the State

The state is always stored in MongoDB in the collection with the name `<application>-<type>
[-<environment>]`. For better observability you can also store the events, commands, etc. by adding a few parts to your application as in the following example. It assumes you use the `environment` configuration setting. If that is not the case then you should remove the `-${ENV}` suffixes. The `AGGREGATE_TYPE` parameter would be the full aggregate type. You can also see that the parts use the predefined aggregate streams.

```yaml
---
- type: stream
  name: save-commands
  fromStream: "${AGGREGATE_TYPE}-command"
  pipeline:
    - $set:
        _id:
          id: $_id
          corr: $_corr
          command: $_command
    - $out: "${AGGREGATE_TYPE}-command-${ENV}"
- type: stream
  name: save-events
  fromStream: "${AGGREGATE_TYPE}-event"
  pipeline:
    - $set:
        _id:
          id: $_id
          seq: $_seq
    - $out: "${AGGREGATE_TYPE}-event-${ENV}"
- type: stream
  name: save-deleted
  fromStream: "${AGGREGATE_TYPE}-aggregate"
  pipeline:
    - $match:
        _deleted: true
    - $out: "${AGGREGATE_TYPE}-deleted-${ENV}"
- type: stream
  name: save-invalid
  fromStream: "${AGGREGATE_TYPE}-reply"
  pipeline:
    - $match:
        _error: true
    - $set:
        _id:
          id: $_id
          corr: $_corr
          command: $_command
    - $out: "${AGGREGATE_TYPE}-invalid-${ENV}"
```

## Uniqueness

Sometimes it is important that aggregate instances are unique according to some business criterion and not only the aggregate identifier. This can be achieved by giving the aggregate object a MongoDB expression that generates unique values from the commands. Commands with a different aggregate identifier but the same unique business value will map to the same aggregate instance. The aggregate identifier of the instance will be the aggregate identifier of the first command with the unique value.

Whether this is useful or not depends on the use-case. When you have a stream of objects that come from another system and where the desired uniqueness has no meaning then this feature makes it very easy to consolidate that stream correctly in aggregate instances. However, when duplicates could be created accidentally, this feature would promote the overwriting of data from different users. In that case it is better to add a unique index to the MongoDB aggregate collection.

When you use this feature, the Kafka topic with the purpose `unique` should exist. You should also make sure all commands have the fields that constitute the unique expression.

Unique expressions involving only one scalar field are very simple. The expression is then `"$<field-name>"`. Sometimes, however, uniqueness can depend on several fields. You could write an expression that turns this into some scalar value, but that wouldn't be efficient. You could not make use of a MongoDB index. Therefore, a unique expression is also allowed to be a JSON object. You would write the expression like this:

```json
{
  "field1": "$field1",
  "field2": "$field2"  
}
```

As with scalar values the field references are used to extract the values from the command. The result is a plain equality expression for MongoDB.

## Access Control

Next to role-based and function-based access control it is sometimes also necessary to manage access to individual aggregate instances. A user may, for example, be allowed to issue a certain command to a certain type of aggregate, but not necessarily to all instances of that type.

The aggregate part checks if the field `_acl` is present in the current state of an aggregate instance. Without the field the command is allowed. Otherwise it should contain a subobject, where each field is the name of a command. The value is an array of role names, which is matched with the field `/_jwt/roles` in the command. If the intersection is not empty then the command is good to go. If there is no field in `_acl` that corresponds to the command then the fallback field `write` is tried. If that is also not available then the command is allowed. If the `/_jwt/sub` field is `system`, then the command is always allowed.

In the following example there are constraints for the commands `get` and `put`.

```json
{
  ...
  "_acl": {
    "get": ["get_role"],
    "put": ["put_role"]    
  }
  ...  
}
```

The following command would be denied because the user who sent it doesn't have the `put_role`:

```json
{
  "_id": "21f38d06-cbc2-4ec4-9c1c-271285776cc7",
  "_corr": "e4179cf3-886a-4457-8d18-94cf0db7f760",
  "_type": "plusminus-counter",  
  "_command": "put",
  "_jwt": {
    "sub": "memyselfandi",
    "roles": ["get_role"]    
  },
  "value": 0  
}
```

## The HTTP API

JSON Streams doesn't have an API. It only works with Kafka topics. It is, however, possible to interact with managed state in a generic way. The repository [pincette-jes-http](https://github.com/json-event-sourcing/pincette-jes-http) is an example of an HTTP-server that provides this. It's a light weight process you can spin up with a port.


## An Example

The following example is the aggregate `plusminus-counter`. It has the commands `plus`, `minus` and `put`. All have an effect on the field `value`.

```yaml
---
application: plusminus
version: "1.0"
parts:
  - type: aggregate
    aggregateType: plusminus-counter
    commands:
      plus:
        reducer: plus.jslt
        validator: validate_plus.yml
      minus:
        reducer: minus.jslt
        validator: validate_minus.yml
      put:
        reducer: put.jslt
        validator: validate_put.yml
```      

plus.jslt:

```
.state | {
  "value": .value + 1,
  *: .
}
```

minus.jslt:

```
.state | {
  "value": .value - 1,
  *: .
}
```

put.jslt:

```
.command | {
  "_command": null,
  *: .
}
```

validate_plus.yml:

```yaml
---
include:
  - operator.yml
conditions:
  - _command: plus
```

validate_minus.yml:

```yaml
---
include:
  - operator.yml
conditions:
  - _command: minus
```

validate_put.yml:

```yaml
---
include:
  - type.yml
conditions:
  - _command: put
  - value: 0
    $code: INIT
```

type.yml:

```yaml
---
conditions:
  - _type: plusminus-counter
```

operator.yml:

```yaml
---
include:
  - type.yml
conditions:
  - value:
      $exists: false
    $code: OPERATOR
```

The first two reducers move the context into the `state` field. They change the `value` field and just copy all the others. The last reducer is for the `put` command. It moves the context into the `command` field, because the whole command will become the new state. Only the `_command` field is removed.

You don't have to provide the `put` command, because it is built in. The reason to add it here is validation. You could also import a common `put` implementation in JSLT or JQ.

The validators check the command names and if the commands are for the right aggregate type. The latter is not strictly necessary, because commands of the wrong type are ignored by the aggregate. For the commands `plus` and `minus` an extra check is done. The `value` field should not be present. In the case of the `put` command the `value` must be zero.

