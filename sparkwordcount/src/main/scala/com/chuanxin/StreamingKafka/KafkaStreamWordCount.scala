package com.chuanxin.StreamingKafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Huang on 2018/11/1 0001.
 */
object KafkaStreamWordCount {

  def main(args: Array[String]) {



    val conf: SparkConf = new SparkConf().setAppName("KafkaStreamWordCount").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    val groupId = "mmm"

    val zks = "master:2181,slave1:2181,slave2:2181"

    val topic = Map[String,Int]("migu"->1)


    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zks,groupId,topic)

    val lines: DStream[String] = data.map(_._2)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordOne: DStream[(String, Int)] = words.map((_,1))

    val result: DStream[(String, Int)] = wordOne.reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
