# Auto-scaling

JSON Streams supports auto-scaling based on the desired maximum message lag of an application on Kafka topics. If you declare such a constraint then it will be used to calculate the desired number of instances. Otherwise messages may pile up, which is many cases is just fine.

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

For faster start-up it is best to set the minimum number of instances so that all applications can be run. Otherwise it will take a few rounds to ramp up.

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