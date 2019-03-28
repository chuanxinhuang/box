package com.chuanxin.StreamingKafka

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Huang on 2018/11/1 0001.
 */
object StatefulKafkaWordCount {

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
//    iter.map{ case(x, y, z) => (x, y.sum + z.getOrElse(0))}
  }

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("StatefulKafkaWordCount").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(conf,Seconds(10))


    //ssc : org.apache.spark.streaming.StreamingContext,
    // zkQuorum : scala.Predef.String,
    // groupId : scala.Predef.String,
    // topics : scala.Predef.Map[scala.Predef.String

    val zkQuorum = "master:2181,slave1:2181,slave2:2181"
    val groupId = "g10"
    val topics = Map[String,Int]("migu"->1)
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topics)

    val lines: DStream[String] = data.map(_._2)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordOne: DStream[(String, Int)] = words.map((_,1))

//    val result: DStream[(String, Int)] = wordOne.reduceByKey(_+_)


    val result: DStream[(String, Int)] = wordOne.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    result.print()

    ssc.checkpoint("E:\\logs\\ck")

    ssc.start()

    ssc.awaitTermination()

  }

}
