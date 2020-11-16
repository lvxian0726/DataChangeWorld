package com.lvxian.streaming

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object KafkaToRedis {


  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    val function = new SocketTextStreamFunction("192.168.70.1", 1234, "\n", 5)


     val data: DataStream[(String, Int)] = environment.addSource(function).map((_,1))

    val value: DataStream[(String, Int)] = data.keyBy(0).timeWindow(Time.seconds(5)).aggregate(customAggreation)

    value.print()


//    def customMap(value: String) = {
//      println("input data:" + value)
//      val json: JSONObject = JSON.parseObject(value)
//      val name: String = json.getString("name")
//      val news_id: String = json.getString("news_id")
//      val click: Int = json.getInteger("click")
//      val show: Int = json.getInteger("show")
//
//      val jedis = new Jedis("192.168.70.128", 6379)
//      jedis.select(0)
//
//      val flag = if (click == 1) "1" else "0"
//
//      jedis.hsetnx(name, news_id, flag)
//      jedis.expire(name, 60 * 3)
//      jedis.close()
//      ""
//    }
//
////    val trans: DataStream[String] = data.map(customMap(_))
//
//
//    val richMap: DataStream[String] = data.map(new RichMapFunction[String, String] {
//
//      var jedis: Jedis = _
//
//      override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)
//
//      override def getRuntimeContext: RuntimeContext = super.getRuntimeContext
//
//      override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext
//
//      override def open(parameters: Configuration): Unit = {
//        println("rich map function init start...")
//        jedis = new Jedis("192.168.70.128", 6379)
//        jedis.select(0)
//        super.open(parameters)
//      }
//
//      override def close(): Unit = {
//        println("rich map function end...")
//        jedis.close()
//        super.close()
//      }
//
//      override def map(value: String): String = {
//        println("input data:" + value)
//        val json: JSONObject = JSON.parseObject(value)
//        val name: String = json.getString("name")
//        val news_id: String = json.getString("news_id")
//        val click: Int = json.getInteger("click")
//        val show: Int = json.getInteger("show")
//
//        val jedis = new Jedis("192.168.70.128", 6379)
//        jedis.select(0)
//
//        val flag = if (click == 1) "1" else "0"
//        jedis.hsetnx(name, news_id, flag)
//        jedis.expire(name, 60 * 3)
//        jedis.close()
//        ""
//      }
//    })


    environment.execute()
  }


}
