package com.expedia.www

/**
  * Created by sogoyal on 5/24/16.
  */
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

import java.util.Properties
import java.util.concurrent.ExecutionException

class Producer(topic: String, isAsync: Boolean) extends Thread {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "DemoProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[Int, String](props)
  def send(message: String): Unit = {

    val record = new ProducerRecord(topic, Producer.messageNo, message)
    if (isAsync) { // Send asynchronously
      producer.send(record)
    }
    else {
      // Send synchronously
      producer.send(new ProducerRecord(topic, Producer.messageNo, message)).get()
      println("Sent message: (" + Producer.messageNo + ", " + message + ")")
    }
  }
}

object Producer {
  var messageNo: Int = 0
}