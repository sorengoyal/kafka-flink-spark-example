package com.expedia.www

/**
  * Created by sogoyal on 5/24/16.
  */

import java.util.Properties

import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig

class Producer(topic: String, isAsync: Boolean) extends Thread
{
  var messageNo: Int = 0
  val props: Properties  = new Properties()
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("metadata.broker.list", "localhost:9092")
  // Use random partitioner. Don't need the key type. Just set it to Integer.
  // The message is of type String.
  val producer: kafka.javaapi.producer.Producer[Integer, String] = new kafka.javaapi.producer.Producer[Integer, String](new ProducerConfig(props))

  def run(message: String): Unit = {
    while(true) {
      producer.send(new KeyedMessage[Integer, String](topic, message))
      messageNo += 1
    }
  }
}
