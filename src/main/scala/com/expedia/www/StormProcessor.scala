package com.expedia.www

/**
  * Created by sogoyal on 6/6/16.
  */

import java.io.File
import java.util
import java.util.{Properties, UUID}

import backtype.storm.spout.RawMultiScheme
import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.{OutputFieldsDeclarer, TopologyBuilder}
import backtype.storm.tuple.{Fields, Tuple, Values}
import backtype.storm.{Config, LocalCluster}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
//import org.slf4j.Logger
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
//import org.apache.log4j.BasicConfigurator
//import org.slf4j.LoggerFactory
import storm.kafka.bolt.KafkaBolt
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper
import storm.kafka.bolt.selector.DefaultTopicSelector
import storm.kafka.{BrokerHosts, KafkaSpout, SpoutConfig, ZkHosts}

import scala.collection.mutable.HashMap

class MyBolt extends BaseRichBolt {

  var _collector: OutputCollector = null
  var counter = 0
  var key = 1
  val clickMap: HashMap[String, Int] = new HashMap[String, Int].empty
  val impressionMap: HashMap[String, Int] = new HashMap[String, Int].empty

  override def execute(input: Tuple): Unit = {
    val schema: Schema = new Schema.Parser().parse(new File("src/main/avro/conversion-schema.avsc"))
    val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
    val avroRecord: GenericRecord = recordInjection.invert(input.getValue(0).asInstanceOf[Array[Byte]]).get
    //Store elements in a map
    counter = counter + 1
    val customer: String = avroRecord.get("customer").toString
    if(impressionMap.contains(customer))
      impressionMap(customer) += 1
    else
      impressionMap += (customer -> 1)
    if( avroRecord.get("clickTime").toString.toInt != 0) {
      if (clickMap.contains(customer))
        clickMap(customer) += 1
      else
        clickMap += (customer -> 1)
    }
    if(counter == 20) {
      var str: String = ""
      str += "**Clicks**\n"
      for (record <- clickMap) {
        println(record._1 + " " + record._2)
        str = str + record._1 + " " + record._2.toString + "|"
      }
      str += "\n**Impressions**\n"
      for (record <- impressionMap) {
        println(record._1 + " " + record._2)
        str = str + record._1 + " " + record._2.toString + "|"
      }
      _collector.emit(input, new Values(key.toString, str))
      key += 1
      clickMap.clear()
      impressionMap.clear()
      counter = 0
    }
    _collector.ack(input)
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    _collector = collector
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("key","message"))
  }
}

object MyBolt{
  //val LOG: Logger = LoggerFactory.getLogger(MyBolt.getClass)
}
object StormProcessor {
  //val LOG: Logger  = LoggerFactory.getLogger(StormProcessor.getClass)
  def main(args: Array[String]) {

   // BasicConfigurator.configure()

    val cluster: LocalCluster = new LocalCluster()
    val builder: TopologyBuilder = new TopologyBuilder()

    val hosts: BrokerHosts = new ZkHosts("localhost:2181")
    val spoutConfig: SpoutConfig  = new SpoutConfig(hosts, "conversions", "/conversions", UUID.randomUUID().toString())
    spoutConfig.scheme = new RawMultiScheme
    val kafkaSpout: KafkaSpout = new KafkaSpout(spoutConfig)

    val myBolt: MyBolt = new MyBolt
    val kafkaSink: KafkaBolt[String, String] = new KafkaBolt[String, String]
      kafkaSink.withTopicSelector(new DefaultTopicSelector("final"))
      .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper())

    builder.setSpout("kafkaSpout", kafkaSpout)
    builder.setBolt("myBolt", myBolt).shuffleGrouping("kafkaSpout")
    builder.setBolt("kafkaSink", kafkaSink).shuffleGrouping("myBolt")


    val conf: Config = new Config()
    //set producer properties.
    val props: Properties = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("request.required.acks", "1")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props)


    cluster.submitTopology("kafkaboltTest", conf, builder.createTopology())
  }
}
