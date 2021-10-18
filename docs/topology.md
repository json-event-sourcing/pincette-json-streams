# Topology Life Cycle Events

If a topology Kafka topic is set in the configuration then life cycle events will be published to it when application topologies start and stop. The messages contain the application, the action and the external Kafka topics that go in and out of the topology. This is an example:

```
{
  "application": "my-app",
  "in": [
    "app-type-command",
    "app-type-unique"
  ],
  "out": [
    "app-type-event-full",
    "app-type-reply",
    "app-type-event",
    "app-type-unique",
    "app-type-aggregate"
  ],
  "action": "stop"
}
```