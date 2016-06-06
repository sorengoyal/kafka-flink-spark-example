package com.expedia.www

/**
  * Created by sogoyal on 5/26/16.
  */

import java.io.File
import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DatumWriter, EncoderFactory}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082
import org.apache.flink.streaming.connectors.kafka.KafkaSink
import org.apache.flink.streaming.util.serialization.{SerializationSchema, SimpleStringSchema}



object FlinkImpressionsProcessor {

  class Impression(var customer: String, var hotel: String, var timestamp: String)

  val schemaFile = new File("src/main/avro/impression-schema.avsc")
  val schema: Schema = new Schema.Parser().parse(schemaFile)

  object ImpressionSchema extends SerializationSchema[Impression, Array[Byte]] {
    override def serialize(element: Impression): Array[Byte] = {
      val impression: GenericRecord = new GenericData.Record(schema)
      val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)
      impression.put("customer", element.customer)
      impression.put("hotel", element.hotel)
      impression.put("timestamp", element.timestamp)
      val out: ByteArrayOutputStream = new ByteArrayOutputStream
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
      writer.write(impression, encoder)
      encoder.flush()
      out.close()
      val output = out.toByteArray()
      println("structuredImpressions:" + element)
      return output
    }
  }

  class StringToImpression extends MapFunction[String, Impression] {
    override def map(element: String): Impression = {
      val strs: Array[String] = element.split(" ")
      new Impression(strs(0), strs(1), strs(2))
    }
  }

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "myGroup")
    env.addSource(new FlinkKafkaConsumer082("impressions", new SimpleStringSchema, properties)) //, OffsetStore.KAFKA, FetcherType.LEGACY_LOW_LEVEL  ))
      .map(new StringToImpression)
      .addSink(new KafkaSink[Impression]("localhost:9092", "structuredImpressions", ImpressionSchema ))
    env.execute()
  }
}
