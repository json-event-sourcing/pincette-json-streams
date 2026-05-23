# Local Development

To run JSON Streams and its auxiliary parts on your local computer, you can use a
[kind](https://kind.sigs.k8s.io) Kubernetes cluster with the following configuration:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30092
        hostPort: 9094 # kafka
        listenAddress: "127.0.0.1"
        protocol: TCP
      - containerPort: 30017
        hostPort: 27018 # mongo
        listenAddress: "127.0.0.1"
        protocol: TCP
      - containerPort: 30020
        hostPort: 9000 # jes-http
        listenAddress: "127.0.0.1"
        protocol: TCP
      - containerPort: 30021
        hostPort: 9001 # sse
        listenAddress: "127.0.0.1"
        protocol: TCP
      - containerPort: 30022
        hostPort: 9002 # letter-box
        listenAddress: "127.0.0.1"
        protocol: TCP
```

The commented host ports are exposed on `localhost`. Create the cluster as follows:

```bash
kind create cluster --config <my-config.yaml>
```

The kubectl context will now be `kind-kind`. Next, you deploy the Helm chart like this:

```bash
helm repo add wdonne https://wdonne.github.io/helm
helm repo update
helm install json-streams-dev wdonne/json-streams-dev
```

Check out the [values file](https://github.com/wdonne/helm/blob/main/json-streams-dev/values.yaml) 
to learn how you can change the configuration.

You will also need the JSON Streams CLI and another tool with utilities for Kafka topics and 
MongoDB collections. You can install them through brew:

```bash
brew tap wdonne/tap
brew install json-streams
brew install json-streams-kit
```

To interact with the [HTTP endpoint](https://github.com/json-event-sourcing/pincette-jes-http) 
or the [Server-Sent Events endpoint](https://github.com/wdonne/pincette-kafka-sse), the user 
should be known. You can use the following fake JWT and set it as either a bearer token or as 
the value of the cookie `access_token`. It sets the username to `system`.

```
eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzeXN0ZW0iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJleHAiOjIyMjExMzY4ODUsImlhdCI6MTc3OTI4NzI4NX0.YP_fUGe7_9l_GGHTO5Ec6ZGYhL_rD4IiiaYxEZrX9aA
```

The [plusminus app](https://github.com/json-event-sourcing/pincette-json-streams/tree/master/apps
/plusminus) allows you to play with the environment. Download it and then install it with:

```bash
json-streams build -c <my-config.conf> -f plusminus/application.yaml
```

You can set the value of an aggregate instance by posting a command to `jes-http` like this:

```json
{
  "_id": "d349d19a-b323-4abc-8ab9-c9a897339591",
  "_corr": "1f6f5358-aa8a-4cc1-a8e5-292fb896fe9e",
  "_type": "plusminus-counter",
  "_command": "put",
  "value": 0
}
```

Assuming that command is in the file `put.json`, you can post it like this:

```bash
curl -i -X POST http://localhost:9000/plusminus/counter/d349d19a-b323-4abc-8ab9-c9a897339591 \
-H "Cookie: access_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzeXN0ZW0iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJleHAiOjIyMjExMzY4ODUsImlhdCI6MTc3OTI4NzI4NX0.YP_fUGe7_9l_GGHTO5Ec6ZGYhL_rD4IiiaYxEZrX9aA" \
--data-binary "@put.json"
```

You can watch on the SSE-side with the following command:

```bash
curl -i http://localhost:9001 \
-H "Cookie: access_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzeXN0ZW0iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJleHAiOjIyMjExMzY4ODUsImlhdCI6MTc3OTI4NzI4NX0.YP_fUGe7_9l_GGHTO5Ec6ZGYhL_rD4IiiaYxEZrX9aA"
```

Remember that no event will come out if the command doesn't change anything to the aggregate 
instance. You should also not reuse correlation IDs.
