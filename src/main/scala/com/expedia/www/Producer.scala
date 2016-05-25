package com.expedia.www

/**
  * Created by sogoyal on 5/24/16.
  */

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties


class Producer(topic: String, isAsync: Boolean) extends Thread {
  var messageNo: Int = 0
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "DemoProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val kproducer = new KafkaProducer[Int, String](props)
  def run(message: String): Unit = {
    val record = new ProducerRecord(topic, messageNo, message)
    if (isAsync) { // Send asynchronously
      kproducer.send(record)
    }
    else {
      // Send synchronously
      kproducer.send(new ProducerRecord(topic, messageNo, message)).get()
      println("Sent message: (" + messageNo + ", " + message + ")")
    }
    messageNo += 1
  }
  override def toString() = {
    topic
  }
}
