package com.expedia.www

import java.io.File
import java.util.Properties

import collection.immutable.HashMap
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
/**
  * Created by sogoyal on 5/31/16.
  */

class Conversion(customer: String, hotel: String, impressionTime: Long, clickTime: Long )


object SparkProcessor {

  class Tag(val customer: String, val hotel: String ) extends Serializable {

  }


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkProcessor").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val topics = Array("structuredImpressions", "structuredClicks").map((_,1)).toMap
    //val clicksTopic = HashMap("structuredClicks" -> 2)
    //val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaParams = ("bootstrap.servers" -> "localhost:9092")
    val impressionsStream = KafkaUtils.createStream(ssc, "localhost:2181", "myGroup", topics).map(_._2)
    //val clicksStream = KafkaUtils.createStream(ssc, "localhost:2181", "myGroup", clicksTopic).map(_._2)
    val imp = impressionsStream.map( (input ) => {
        val schema: Schema = new Schema.Parser().parse(new File("src/main/avro/impression-schema.avsc"))
        val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
        val record: GenericRecord = recordInjection.invert(input.getBytes()).get //val record: GenericRecord = recordInjection.invert(avroRecord._2).get()
        //println(record.get("customer").toString + record.get("hotel").toString + record.get("timestamp").toString)
      (record.get("customer").toString.length.toString + record.get("customer").toString + record.get("hotel").toString, record.get("timestamp").toString)
    })
    val out = imp.groupByKey().map( (record) => {
      val schema: Schema = new Schema.Parser().parse(new File("src/main/avro/conversion-schema.avsc"))
      val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
      val avroRecord: GenericRecord = new GenericData.Record(schema)
      println("[LOG]Record:" + record._1 + "|" + record._2.toString())
      val hotelIndex = record._1.charAt(0) - '0' + 1
      println("Hotel Index:" + hotelIndex)
      avroRecord.put("customer", record._1.substring(1, hotelIndex))
      avroRecord.put("hotel", record._1.substring(hotelIndex))
      val times = record._2.toArray
      println("Times:" + times(0) + times(1))
      avroRecord.put("impressionTime", times(0))
      avroRecord.put("clickTime", times(1))
      //println("AvrorecordavroRecord.toString)
      val output: Array[Byte] = recordInjection.apply(avroRecord)
      output
    })

    out.foreachRDD(rdd => {
      val props: Properties  = new Properties()
      //props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("metadata.broker.list", "localhost:9092")
      val producer = new Producer[Integer, Array[Byte]](new ProducerConfig(props))
      for (msg <- rdd.collect ) {
        producer.send(new KeyedMessage[Integer, Array[Byte]]("conversions", msg))
      }
      producer.close
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
