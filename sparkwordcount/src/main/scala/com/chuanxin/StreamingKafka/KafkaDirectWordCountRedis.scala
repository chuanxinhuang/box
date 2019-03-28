package com.chuanxin.StreamingKafka

import com.chuanxin.utils.JedisPools
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * Created by Huang on 2018/11/2 0002.
 */
object KafkaDirectWordCountRedis {

  def main(args: Array[String]) {

    val jedisConn: Jedis = JedisPools.getConnection()

    val conf: SparkConf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[*]")

    val streamingContext = new  StreamingContext(conf,Duration(5000))

    val group = "m001"

    val topic ="migu"

    val brokerlist = "master:9092,slave1:9092,slave2:9092"

    val topics: Set[String] = Set(topic)

    val redisTopocPath = s"${group}:${topic}"

    //准备kafka的参数
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerlist,
      "group.id" -> group,
      //从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    var kafkaStream: InputDStream[(String, String)] = null
    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()


    val keys: Array[AnyRef] = jedisConn.keys(s"${redisTopocPath}*").toArray
    println(redisTopocPath)
    println(keys.length)

    if (!keys.isEmpty){

      for( i <- 0 until keys.length ){

        val value: String = keys(i).asInstanceOf[String]

        val index: Int = value.lastIndexOf(":")

        val op: String = value.substring(index+1)

        val partitionOffset = jedisConn.get(value.asInstanceOf[String])

        val tp = TopicAndPartition(topic,op.toInt)

        fromOffsets += (tp -> partitionOffset.toLong)
      }
       val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streamingContext, kafkaParams, fromOffsets, messageHandler)
    }else{

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics)
    }

    //偏移量的范围,查看原代码可看到OffsetRange里面记录了topic,partition,fromOffset,untilOffset等信息, 也就是主题, 分区和偏移量
//    final class OffsetRange private (val topic : scala.Predef.String, val partition : scala.Int, val fromOffset : scala.Long, val untilOffset : scala.Long)
    var offsetRanges = Array[OffsetRange]()

    kafkaStream.foreachRDD { kafkaRDD =>
      //只有KafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
      offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      val lines: RDD[String] = kafkaRDD.map(_._2)

      //对RDD进行操作，触发Action
      lines.foreachPartition(partition =>
        partition.foreach(x => {
          println(x)
        })
      )
      for (o <- offsetRanges) {
        //将该 partition 的 offset 保存到 redis ,也是set方法
        jedisConn.set(s"${redisTopocPath}:${o.partition}",o.untilOffset.toString)
      }
    }
    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
