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
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-compiler" % "2.10.6",
      "org.scala-lang" % "scala-reflect" % "2.10.6",
      "org.apache.kafka" % "kafka_2.10" % "0.8.2.1",
      "org.apache.kafka" % "kafka-clients" % "0.8.2.1",
      "org.apache.flink" % "flink-connector-kafka" % "0.9.1",
      "org.apache.flink" % "flink-streaming-scala" % "0.9.1", //flink streaming API
      "org.apache.avro" % "avro" % "1.7.7",
      "com.twitter" % "bijection-avro_2.10" % "0.9.2",
      "org.apache.spark" % "spark-core_2.10" % "1.5.1",
      "org.apache.spark" % "spark-streaming_2.10" % "1.5.1",
      "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.1",
      "org.apache.storm" % "storm-core" % "0.10.1",
      "org.apache.storm" % "storm-kafka" % "0.10.0",
      "log4j" % "log4j" % "1.2.17"
      //"org.slf4j" % "slf4j-simple" % "1.7.5"
    )
  )
}
