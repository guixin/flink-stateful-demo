
### kafka command

#### create topic
```sh
bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic names --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic greetings --bootstrap-server localhost:9092
```

```sh
bin/kafka-console-producer.sh --topic names --bootstrap-server localhost:9092
bin/kafka-console-consumer.sh --topic names --from-beginning --bootstrap-server localhost:9092

bin/kafka-console-consumer.sh --topic greetings --from-beginning --bootstrap-server localhost:9092
```