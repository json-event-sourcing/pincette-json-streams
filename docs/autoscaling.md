# Auto-scaling

JSON Streams supports auto-scaling based on the desired maximum message lag of an application on Kafka topics. If you declare such a constraint then it will be used to calculate the desired number of instances. Otherwise messages may pile up, which in many cases is just fine.

You create message lag constraints by uploading a JSON document in the same collection as the one that contains all the compiled applications. The document should look like this:

```json
{
  "_id": "ea07767d-0766-4aed-b8f6-f14e57ad015e",
  "maximumMessageLag": {
    "myapplication": {
      "myhottopic": 100,
      "mywarmtopic": 1000      
    }    
  }  
}
```

This says that the application `myapplication` should not leave more than 100 messages unprocessed for the topic `myhottopic` and no more than 1000 for `mywarmtopic`.

The leader of the JSON Streams instances will calculate the overall excess message lag and combine it with all the present applications to derive the number of instances. For those applications that couldn't be started there is a contribution to the excess message lag that is based on the `work.averageMessageTimeEstimate` configuration entry. Excess JSON Streams instance capacity is also calculated with that entry.

The leader will spread the available work evenly across all available instances. At some point an application will have reached its maximum number of instances. This is defined by the highest number of topic partitions for any topic the application is consuming. Extra instances would be idle in the respective Kafka consumer groups.

For faster start-up it is best to set the minimum number of instances in such a way that all applications can be run. Otherwise it will take a few rounds to ramp up.

If the configuration has set the entry `work.excessMessageLagTopic`, then the desired number of instances will be published on it. The current number of instances is also put in the message. This gives the relative change that is needed. A published message looks like this:

```json
{
  "_id": "421974db-c29d-4eda-be4c-71578089b2ef",
  "desired": 20,
  "running": 18,  
  "time": "2021-10-18T16:20:34.367Z"  
}
```

There are two ways to consume those messages. Either some standardised custom metric is calculated with a message, which can be fed into an auto-scaler. Or it is used to set the number of instances directly.

The former approach can be achieved easily with [KEDA](https://keda.sh). A small [JSON Streams application](https://github.com/json-event-sourcing/pincette-json-streams/tree/master/apps/autoscaling) can turn an excess message lag message into an OpenTelemetry metric. The following `ScaledObject` resource uses that metric as an external metric for the HPA resource it generates:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: json-streams
  namespace: my-namespace
spec:
  scaleTargetRef:
    name: json-streams
  minReplicaCount: 1
  maxReplicaCount: 32
  triggers:
    - type: prometheus
      metricType: Value
      metadata:
        serverAddress: http://main-kube-prometheus-stack-prometheus.prometheus:9090
        query: (last_over_time(json_streams_instance_skew_ratio{service_namespace="a_namespace"}[1m]) + 31)
        threshold: "31"
```

This says that scaling can occur between 1 and 32 instances. The metric that is used hovers 0, because there can be more or less running instances than the number of desired ones. Therefore, we must shift the value up by 31 to obtain a positive value. However, this reduces the percentage of the deviation, which may have an impact on the behaviour of the generated HPA resource. The latter takes a margin of 10 percent before it starts scaling in or out. In Kubernetes 1.33 there are the fields `behavior.scaleDown.tolerance` and `behavior.scaleUp.tolerance`, with which you can change that percentage. However, these fields are still in alpha and are not active by default.

By default JSON Streams instances are identified by a generated UUID. This can be overridden with the environment variable `INSTANCE`. If you run it in a Kubernetes pod, you can use the downward API with the pod name. This way it is possible to find which applications are running in which pod.