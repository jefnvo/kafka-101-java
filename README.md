# kafka-101-java

Run zookeper:
```
zookeeper-server-start.sh config/zookeeper.properties 
```

Run kafka:
```
kafka-server-start.sh config/server.properties 
```

Create topic:

```
kafka-topics.sh --bootstrap-server localhost:9092 --topic firstTopic --create --partitions 3 --replication-factor 1
```

Create consumer with keys: (guarantees order in a partition)

```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic firstTopic --from-beginning --property print.key=true --property key.separator=,
```

Create producer:
```
kafka-console-producer.sh  --broker-list 127.0.0.1:9092 --topic firstTopic --property parse.key=true --property key.separator=,
```
