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
      scalaVersion := "2.10.2"
      // add other settings here
    )
  )
}
