import sbt._
import sbt.Keys._

object KafkaflinksparkexampleBuild extends Build {

  lazy val kafkaflinksparkexample = Project(
    id = "kafka-flink-spark-example",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "kafka-flink-spark-example",
      organization := "com.expedia.www",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.6"
      // add other settings here
    )
  ).settings(
    // http://mvnrepository.com/artifact/org.apache.kafka/kafka_2.10
    libraryDependencies ++= Seq(
        "org.apache.kafka" % "kafka_2.10" % "0.8.2.0",
        // http://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
        "org.apache.kafka" % "kafka-clients" % "0.8.2.0",
        "org.apache.flink" % "flink-connector-kafka" % "0.9.1",
        "org.apache.flink" % "flink-streaming-scala" % "0.9.1", //flink streaming API
        "org.apache.avro" % "avro" % "1.7.7"
    )
  )
}
