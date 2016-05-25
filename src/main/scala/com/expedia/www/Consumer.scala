package com.expedia.www

/**
  * Created by sogoyal on 5/25/16.
  */
//TODO:Not working. But not necessary anyways
import kafka.utils.ShutdownableThread
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

import java.util.Collections
import java.util.Properties

class Consumer(topic: String) extends ShutdownableThread(topic, false) {
  var numOfMessages: Int = 0
  val props = new Properties
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
  props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  val kconsumer = new KafkaConsumer[Int, String](props)
  kconsumer.subscribe(Collections.singletonList(topic))
  override def doWork() = {
    val records = kconsumer.poll(100).asScala
    for (record <- records) {
      numOfMessages += 1
      println("Received message: (" + topic + "," + record.key() + ", " + record.value() + ")" )
    }
  }

  override def toString = {
    topic
  }
}
