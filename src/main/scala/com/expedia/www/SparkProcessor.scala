package com.expedia.www

import java.io.File
import java.util.HashMap

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._


/**
  * Created by sogoyal on 5/31/16.
  */


object SparkProcessor {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkProcessor").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = Array("structuredClicks","structuredImpressions").map((_, 1)).toMap


    //Set<String> topics = Collections.singleton("mytopic");
    val kafkaParams: HashMap[String, String] = new HashMap[String, String]()
    kafkaParams.put("bootstrap.servers", "localhost:9092")
    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "myGroup", topicMap).map(_._2)
    kafkaStream.foreachRDD( (rdd ) => {
      rdd.foreach(avroRecord => {
        val schema: Schema = new Schema.Parser().parse(new File("src/main/avro/impression-schema-spark.avsc"))
        val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
        val record: GenericRecord = recordInjection.invert(avroRecord.getBytes()).get //val record: GenericRecord = recordInjection.invert(avroRecord._2).get()
        println(record)
        //println(record.get("customerid") + record.get("hotelid") + record.get("timestamp"))
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
