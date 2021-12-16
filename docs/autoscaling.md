# Auto-scaling

JSON Streams supports auto-scaling based on the desired maximum message lag of an application on Kafka topics. If you declare such a constraint then it will be used to calculate the excess message lag. Otherwise messages may pile up, which is many cases is just fine.

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

The leader of the JSON Streams instances will calculate the overall excess message lag. The number of messages above the constraints are added to it. Then there is a maximum number of applications per instance. So, there should be enough instances to run all the applications. For those applications that couldn't be started there is a contribution to the excess message lag that is based on the `work.averageMessageTimeEstimate` configuration entry. Excess JSON Streams instance capacity is also calculated with that entry. It will add negative values.

The leader will spread the available work evenly across all available instances. At some point an application will have reached its maximum number of instances. This is defined by the highest number of topic partitions for any topic the application is consuming. Extra instances would be idle in the respective Kafka consumer groups. If there is still message lag the number of JSON Streams instances can be grown in order to spread the applications thinner. Each instance will have less applications to run. Instances of the same application will also be spread over different JSON Streams instances.

You should not set the maximum number of applications per instance to more than 10, which is the default. Otherwise there will be "too many open files" issues with Kafka Streams. You can also set the `ulimit` `nofile` higher, for example to 4096. The default for Docker is 1024.

For faster start-up it is best to set the minimum number of instances so that all applications can be run. Otherwise it will take a few rounds to ramp up.

If the configuration has set the entry `work.excessMessageLagTopic`, then the overall excess message lag will be published on it. The figure is normalised between 0 and 100. To make this work you need to set the configuration entry `work.maximumInstances`. This can be used by auto-scalers as a custom metric. The target value should be 50. A published message looks like this:

```json
{
  "_id": "421974db-c29d-4eda-be4c-71578089b2ef",
  "excessMessageLag": 75,
  "time": "2021-10-18T16:20:34.367Z"  
}
```

The above example suggests to the auto-scaler that the number of instances should be increased by 50 percent.