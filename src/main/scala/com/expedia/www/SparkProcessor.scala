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


class MyKafkaSink(createProducer: () => Producer[Integer, Array[Byte]]) extends Serializable {
  lazy val producer = createProducer()
  def send(topic: String, message: Array[Byte]): Unit = producer.send(new KeyedMessage[Integer, Array[Byte]](topic, message))
}

object MyKafkaSink {
  def apply(props: Properties): MyKafkaSink = {
    val f = () => {
      val producer = new Producer[Integer, Array[Byte]](new ProducerConfig(props))
      sys.addShutdownHook {
        producer.close
      }
      producer
    }
    new MyKafkaSink(f)
  }
}

object SparkProcessor {

  class Tag(val customer: String, val hotel: String )


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkProcessor").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val impressionTopic = HashMap("structuredImpressions" -> 1)
    val clicksTopic = HashMap("structuredClicks4445" -> 1)
    val kafkaParams = ("bootstrap.servers" -> "localhost:9092")
    val impressionsStream = KafkaUtils.createStream(ssc, "localhost:2181", "myGroup", impressionTopic).map(_._2)
    val clicksStream = KafkaUtils.createStream(ssc, "localhost:2181", "myGroup", clicksTopic).map(_._2)
    val imp = impressionsStream.map( (input ) => {
        val schema: Schema = new Schema.Parser().parse(new File("src/main/avro/impression-schema.avsc"))
        val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
        println(input)
        val record: GenericRecord = recordInjection.invert(input.getBytes()).get //val record: GenericRecord = recordInjection.invert(avroRecord._2).get()
        //println(record)
        //println(record.get("customer").toString + record.get("hotel").toString + record.get("timestamp").toString)
      (new Tag(record.get("customer").toString, record.get("hotel").toString), record.get("timestamp").toString)
    })
    val clk = clicksStream.map((input: String) => {
      val schema: Schema = new Schema.Parser().parse(new File("src/main/avro/click-schema.avsc"))
      val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
      val record: GenericRecord = recordInjection.invert(input.getBytes()).get //val record: GenericRecord = recordInjection.invert(avroRecord._2).get()
      //println(record)
      //println(record.get("customer").toString + record.get("hotel").toString + record.get("timestamp").toString)
      (new Tag(record.get("customer").toString, record.get("hotel").toString), record.get("timestamp").toString)
    })

    val props: Properties  = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list", "localhost:9092")

    val kafkaSink = ssc.sparkContext.broadcast(MyKafkaSink(props))
    val out = imp.join(clk).foreachRDD(rdd => {
      rdd.foreach((record) => {
        val schema: Schema = new Schema.Parser().parse(new File("src/main/avro/conversion-schema.avsc"))
        val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
        val avroRecord: GenericRecord = new GenericData.Record(schema)
        print(record)
        avroRecord.put("customer", record._1.customer)
        avroRecord.put("hotel", record._1.hotel)
        avroRecord.put("impressionTime", record._2._1)
        avroRecord.put("clickTime", record._2._2)
        println(avroRecord)
        val output: Array[Byte] = recordInjection.apply(avroRecord)
        kafkaSink.value.send("conversions", output)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
