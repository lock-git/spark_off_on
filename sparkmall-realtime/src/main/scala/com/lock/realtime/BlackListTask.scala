package com.lock.realtime

import java.util

import com.atguigu.bigdata.sparkmall.common.util.RedisUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * author  Lock.xia
  * Date 2021-03-18
  */
object BlackListTask {

  val topic = ""

  def main(args: Array[String]): Unit = {

    // spark streaming 上下文环境
    val sparkConf: SparkConf = new SparkConf().setAppName("BlackListTask").set("", "") // spark 配置
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // kafka 消费
    val kafkaParams = Map(
      "" -> "",
      "" -> ""
    )

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams))

    // 业务逻辑

    // transform
    val valueDStream: DStream[String] = kafkaDStream.transform { r: RDD[ConsumerRecord[String, String]] =>
      val kafkaStrRDD: RDD[String] = r.map((_: ConsumerRecord[String, String]).value())
      // 获取黑名单
      val client: Jedis = RedisUtil.getJedisClient
      val blackSet: util.Set[String] = client.smembers("black_list")
      // 放入广播变量
      val blackSetBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(blackSet)

      val filterRDD: RDD[String] = kafkaStrRDD.filter((k: String) => !blackSetBroadcast.value.contains(k))
      filterRDD
    }


    // action
    valueDStream.foreachRDD { rdd: RDD[String] =>

      rdd.foreachPartition { r: Iterator[String] =>
        r.foreach { s: String =>

          println(s)

        }

      }

    }



    //启动

    ssc.start()
    ssc.awaitTermination()


  }


}
