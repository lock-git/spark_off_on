package com.atguigu.bigdata.sparkmall.common.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * author  Lock.xia
  * Date 2020-04-23
  */
object MykafkaUtilTwo {

  def getKafkaParam(group_id: String): Map[String, String] = {
    Map(
      "bootstrap.servers" -> "172.16.1.111:9092,172.16.1.113:9092,172.16.1.115:9092",
      "metadata.broker.list" -> "172.16.1.111:9092,172.16.1.113:9092,172.16.1.115:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> group_id,
      "auto.offset.reset" -> "latest",
      "max.partition.fetch.bytes" -> "50000000",
      "enable.auto.commit" -> "true"
    )
  }

  // 创建DStream，返回接收到的输入数据
  // LocationStrategies：根据给定的主题和集群地址创建consumer
  // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
  // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
  // ConsumerStrategies.Subscribe：订阅一系列主题
  def getKafkaStream(group_id: String, topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic),
        getKafkaParam(group_id)))
    dStream
  }

}
