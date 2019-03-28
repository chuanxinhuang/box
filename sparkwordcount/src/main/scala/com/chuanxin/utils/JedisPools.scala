package com.chuanxin.utils

import java.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisPools {

  val config = new JedisPoolConfig()
  //最大连接数,
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  //10000代表超时时间（10秒）
  val pool = new JedisPool(config, "192.168.16.72", 6379, 10000, "xxx")

  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {

    val jedis: Jedis = getConnection()

    //    jedis.set("age","21")
    //    jedis.set("user:vip:name","wangwu")


    //val keys: util.Set[String] = jedis.keys("user*")
    //    println(jedis.get("user:vip:name"))
    //
    //    println(strings.toString)
    val set: util.Set[String] = jedis.keys("m001:migu*")



    val keys: Array[AnyRef] = jedis.keys("m001:migu*").toArray
    println(keys.length)

    if (!keys.isEmpty){

      for( i <- 0 until keys.length ){

        println(i)
        println("------------------------------------------")

        //        val value = keys(i)
        val value: String = keys(i).asInstanceOf[String]


        val index: Int = value.lastIndexOf(":")

        val str: String = value.substring(index+1)
        println(str)

      }
    }

    //     val value: Array[AnyRef] = keys.toArray()
    //    for( i <- 0 until value.length ){
    //
    //      val valu = value(i)
    //
    //      println(valu)
    //    }
    println("-------------------------------")


    jedis.close()
  }
}
