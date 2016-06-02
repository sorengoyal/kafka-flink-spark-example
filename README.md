# kafka-flink-spark-example
An example to understand the Big Data tools - Kafka, Flink and Spark and their APIs in Scala



To run sbt-run

Kafka
=====
Sources:
[tutorial](http://data-artisans.com/kafka-flink-a-practical-how-to/)
for writing a producer and consumer in Java using Kafka API - Go to path *kafka-0.10.0.0-src/examples/src/main/java/kafka/examples* in  [Kafka 0.10.0.0 src](http://kafka.apache.org/downloads.html)

_Get the binaries of the [Kafka](http://kafka.apache.org/downloads.html)_

_Start zookeeper service_
```bash
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
```
_Start a Kafka Server_
```bash
./bin/kafka-server-start.sh ./config/server.properties
```
_Create a test topic. Call it 'Chicago'_
```bash
./bin/kafka-topics.sh --create --topic Chicago --zookeeper localhost:2181 --partitions 1 --replication-factor 1
```
_Spin up a consumer process that will subscribe to 'Chicago' topic on the Kafka Server_
```bash
./bin/kafka-console-consumer.sh --topic test --zookeeper localhost:2181
```
_Start a producer console_
```bash
./bin/kafka-console-producer.sh --topic test --broker-list localhost:9092
```

Enter text in this console. And it will pop up on the consumer's console

Flink
=====
Source :
[Flink data stream to and form Kafka](https://github.com/dataArtisans/kafka-example/)
[Avro + Flink](https://gist.github.com/StephanEwen/d515e10dd1c609f70bed)
[Better understanding of Flink is at their page](https://ci.apache.org/projects/flink/flink-docs-release-1.0/apis/common/index.html)

Avro
====
[Better API for Avro](http://aseigneurin.github.io/2016/03/04/kafka-spark-avro-producing-and-consuming-avro-messages.html)
Common erros - 
Searialzable is called many times. So the schema must be not be read from inside the function.
The constructor should not contain any lies of code otherwise error -  "KafkaSink sink is not serializable"

Spark
=====
[Kafka + Spark Receive](http://spark.apache.org/docs/latest/streaming-kafka-integration.html)
[Kafka + Spark Produce](http://allegro.tech/2015/08/spark-kafka-integration.html)
[Spark Programmming Guide](http://spark.apache.org/docs/latest/programming-guide.html#overview)
