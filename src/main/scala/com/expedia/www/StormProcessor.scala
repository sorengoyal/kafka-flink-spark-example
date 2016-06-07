package com.expedia.www

/**
  * Created by sogoyal on 6/6/16.
  */

import java.io.File
import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.base.{BaseBasicBolt, BaseRichBolt}
import backtype.storm.{Config, LocalCluster}
import backtype.storm.topology.{BasicOutputCollector, IRichBolt, OutputFieldsDeclarer, TopologyBuilder}
import backtype.storm.tuple.{Fields, Tuple, Values}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import storm.kafka.bolt.KafkaBolt
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper
import storm.kafka.bolt.selector.DefaultTopicSelector
import storm.trident.testing.FixedBatchSpout


class MyBolt extends BaseRichBolt {
  var _collector: OutputCollector = null
  var counter = 0
  val clickMap = Map[String, Int]()
  val impressionsMap = Map[String, Int]()

  override def execute(input: Tuple): Unit = {

    val schema: Schema = new Schema.Parser().parse(new File("src/main/avro/conversion-schema.avsc"))
    val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
    val avroRecord: GenericRecord = recordInjection.invert(input.getValue(0).asInstanceOf[Array[Byte]]).get
    //Store elements in a map
    //Emit the map when count reaches 20
    _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
    _collector.ack(tuple);
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    _collector = collector
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = ???
}
object StormProcessor {

  def main(args: Array[String]) {
    val cluster: LocalCluster = new LocalCluster()
    val builder: TopologyBuilder = new TopologyBuilder()

    val fields: Fields  = new Fields("key", "message")
//    val spout: FixedBatchSpout  = new FixedBatchSpout(
//      fields,
//      4,
//      new Values("storm", "1"),
//      new Values("trident", "1"),
//      new Values("needs", "1"),
//      new Values("javadoc", "1")
//    )
//    spout.setCycle(true)
//    builder.setSpout("spout", spout)

    val processor: MyBolt = new MyBolt
    val kafkaSink: KafkaBolt = new KafkaBolt()
      .withTopicSelector(new DefaultTopicSelector("test"))
      .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper())
    builder.setBolt("forwardToKafka", bolt).shuffleGrouping("spout")


    Config conf = new Config();
    //set producer properties.
    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:9092");
    props.put("request.required.acks", "1");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

    StormSubmitter.submitTopology("kafkaboltTest", conf, builder.createTopology());

  }
}
