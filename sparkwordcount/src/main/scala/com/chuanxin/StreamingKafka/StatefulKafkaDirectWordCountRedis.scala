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
object StatefulKafkaDirectWordCountRedis {

  def main(args: Array[String]) {

    val jedisConn: Jedis = JedisPools.getConnection()

    val conf: SparkConf = new SparkConf().setAppName("StatefulKafkaDirectWordCountRedis").setMaster("local[*]")

    val streamingContext = new  StreamingContext(conf,Duration(5000))

    val group = "m0"

    val topic ="test"

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
    //如果 redis 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    //获取是否有redisTopocPath的key, 根据是否有值判断是否记录过偏移量, 是否第一次读取数据
    //jedis.keys(*)获取的是java util包的Set, 需要转换下
    val keys: Array[AnyRef] = jedisConn.keys(s"${redisTopocPath}*").toArray
    //判断是否为空
    if (!keys.isEmpty){

      for( i <- 0 until keys.length ){
        //强转格式, 原本是AnyRef类型
        val value: String = keys(i).asInstanceOf[String]
        //这里的数组是由Set转化来的, 这里的顺序并没有按照partition的顺序,所以要自己找对应的顺序
        val index: Int = value.lastIndexOf(":")
        val op: String = value.substring(index+1)

        val partitionOffset = jedisConn.get(value.asInstanceOf[String])
        //TopicAndPartition是topic和partition关系的对象, 一定要对应上,
        val tp = TopicAndPartition(topic,op.toInt)

        fromOffsets += (tp -> partitionOffset.toLong)
      }
      //MessageAndMetadata 信息和元数据
       val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streamingContext, kafkaParams, fromOffsets, messageHandler)
    }else{

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics)
    }

    //偏移量的范围
    var offsetRanges = Array[OffsetRange]()
//    kafkaStream.foreachRDD里面的业务逻辑是在Driver端执行
    kafkaStream.foreachRDD { kafkaRDD =>
      //判断当前的kafkaStream中的RDD是否有数据
      if (!kafkaRDD.isEmpty()) {
        //只有KafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
        offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        val lines: RDD[String] = kafkaRDD.map(_._2)

        //      对RDD进行操作，触发Action , foreachPartition()对的是分区内每批次多行数据进行处理 , 注意和partition.foreach区分
        lines.foreachPartition(partition => {
          //foreachPartition的逻辑会在Executor中执行, 需要在Executor中执行获取连接
          val jedis: Jedis = JedisPools.getConnection()
          val toList: List[(String, Int)] = partition.flatMap(_.split(" ")).toArray.groupBy(x => x).mapValues(x => x.length).toList

          if (!toList.isEmpty) {
            for (kv <- toList) {
              //jedis  incrByFloat 如果没有对应的键会新建
              jedis.incrByFloat(s"wordcount:${kv._1}", kv._2)
            }
          }
          jedis.close()
          println(toList.toString())

          //------------------------以下和上面的不共存--------------------------------
          //partition.foreach() 是对分区内的每行数据进行处理
//          partition.foreach(x => { //这的x是每行数据, 不是一个批次的
//            //val jedis: Jedis = JedisPools.getConnection()
//            val keys: Array[String] = x.split(" ")
//
//            val list: List[(String, Int)] = keys.groupBy(x => x).mapValues(x => x.length).toList
//
//            for (kv <- list) {
//              jedis.incrByFloat(kv._1, kv._2)
//            }
//            println(list.toString())
//            jedis.close()
//
//          })
          //------------------------以上和上面的不共存--------------------------------

        }

        )
        for (o <- offsetRanges) {
          //将该 partition 的 offset 保存到 redis ,也是set方法
          jedisConn.set(s"${redisTopocPath}:${o.partition}", o.untilOffset.toString)
        }
      }
    }
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
