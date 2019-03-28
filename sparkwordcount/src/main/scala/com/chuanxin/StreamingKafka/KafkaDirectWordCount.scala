package com.chuanxin.StreamingKafka

import com.chuanxin.utils.JedisPools
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * Created by Huang on 2018/11/2 0002.
 */
object KafkaDirectWordCount {

  def main(args: Array[String]) {

    val jedisConn: Jedis = JedisPools.getConnection()

    val conf: SparkConf = new SparkConf().setAppName("KafkaDirectWordCount2").setMaster("local[*]")

    val streamingContext = new  StreamingContext(conf,Duration(5000))

    val group = "m002"

    val topic ="migu"

    val brokerlist = "master:9092,slave1:9092,slave2:9092"

    val zookeepers = "master:2181,slave1:2181,slave2:2181"

    val topics: Set[String] = Set(topic)

    val topicDirs = new ZKGroupTopicDirs(group,topic)
    val zkTopicPath =s"${topicDirs.consumerOffsetDir}"


    //准备kafka的参数
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerlist,
      "group.id" -> group,
      //从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    val zkClient = new ZkClient(zookeepers)

    val children: Int = zkClient.countChildren(zkTopicPath)

    var kafkaStream: InputDStream[(String, String)] = null

    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    if (children > 0){

      for (i <- 0 until children) {
        // /g001/offsets/wordcount/0/10001

        // /g001/offsets/wordcount/0
        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        // wordcount/0
        val tp = TopicAndPartition(topic, i)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        // wordcount/0 -> 10001
        fromOffsets += (tp -> partitionOffset.toLong)
      }

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

      //通过KafkaUtils创建直连的DStream（fromOffsets参数的作用是:按照前面计算好了的偏移量继续消费数据）
      //[String, String, StringDecoder, StringDecoder,     (String, String)]
      //  key    value    key的解码方式   value的解码方式
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streamingContext, kafkaParams, fromOffsets, messageHandler)

    }else{

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics)

    }
    //偏移量的范围
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
        //  /g001/offsets/wordcount/0
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        //将该 partition 的 offset 保存到 zookeeper
        //  /g001/offsets/wordcount/0/20000
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      }
    }
    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
