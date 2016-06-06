package com.expedia.www

import java.io.File
import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, DatumWriter, EncoderFactory}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer082, KafkaSink}
import org.apache.flink.streaming.util.serialization.{SerializationSchema, SimpleStringSchema}

/**
  * Created by sogoyal on 5/31/16.
  */
object FlinkClicksProcessor {
  class Click(var customer: String, var hotel: String, var timestamp: String)

  val schema: Schema = new Schema.Parser().parse(new File("src/main/avro/click-schema.avsc"))
  val click: GenericRecord = new GenericData.Record(schema)

  object ClickSchema extends SerializationSchema[Click, Array[Byte]] {
    override def serialize(element: Click): Array[Byte] = {

      val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)
      click.put("customer", element.customer)
      click.put("hotel", element.hotel)
      click.put("timestamp", element.timestamp)
      val out: ByteArrayOutputStream = new ByteArrayOutputStream
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
      writer.write(click, encoder)
      encoder.flush()
      out.close()
      val output = out.toByteArray()
      println("structuredClicks:" + element)
      return output
    }
  }

  class StringToImpression extends MapFunction[String, Click] {
    override def map(element: String): Click = {
      val strs: Array[String] = element.split(" ")
      new Click(strs(0), strs(1), strs(2))
    }
  }

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "myGroup")
    env.addSource(new FlinkKafkaConsumer082("clicks", new SimpleStringSchema, properties)) //, OffsetStore.KAFKA, FetcherType.LEGACY_LOW_LEVEL  ))
      .map(new StringToImpression)
      .addSink(new KafkaSink[Click]("localhost:9092", "structuredClicks", ClickSchema ))
    env.execute()
  }
}
