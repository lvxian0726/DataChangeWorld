package com.lvxian.streaming

import java.{lang, util}

import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object flinkEventTimeStatusTest {


  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    //    environment.enableCheckpointing(1000 * 60)
    //    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //    environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(60 * 1000)
    //    environment.getCheckpointConfig.setCheckpointTimeout(10 * 1000)
    //    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //    environment.setStateBackend(new FsStateBackend("hdfs://192.168.70.132:9000/flink-1.11.1-checkpoints", true))

    //    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    val function = new SocketTextStreamFunction("192.168.70.1", 1234, "\n", 5)


    val data: DataStream[(String, String)] = environment.addSource(function).map(value => {
      val strings: Array[String] = value.split(",")
      val word: String = strings(0)
      val event_time: String = strings(1)
      (word, event_time)
    })


    val keyByid: KeyedStream[(String, String), Tuple] = data.keyBy(0)

    val value: DataStream[(String, String)] = keyByid.filter(new CustomFilterFunction)


    value.print()


    environment.execute()
  }


  class CustomFilterFunction extends RichFilterFunction[(String, String)] {

    var current_state: ValueState[Long] = _
    var map_state: MapState[String, Long] = _

    override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

    override def getRuntimeContext: RuntimeContext = {
      super.getRuntimeContext
    }

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)

      val statTTL: StateTtlConfig = StateTtlConfig.newBuilder(Time.minutes(5))
        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
        //        .cleanupFullSnapshot()
        .cleanupIncrementally(10, false)
        .build()

      val descriptor = new ValueStateDescriptor[Long]("max_event_time", classOf[Long])
      val map_event_time_descriptor = new MapStateDescriptor[String, Long]("map_event_time", classOf[String], classOf[Long])

      descriptor.enableTimeToLive(statTTL)
      map_event_time_descriptor.enableTimeToLive(statTTL)

      current_state = getRuntimeContext.getState(descriptor)
      map_state = getRuntimeContext.getMapState(map_event_time_descriptor)

      //      current_state = getRuntimeContext.getState(new ValueStateDescriptor[Long]("max_event_time", classOf[Long]))

    }

    override def close(): Unit = super.close()

    override def filter(value: (String, String)): Boolean = {

      val imei: String = value._1
      val newlest_event_time: Long = value._2.toLong
      var flag: Boolean = false


      //      val current_state_value: Long = current_state.value()
      //      print("当前数据：" + value + "    ----->上一次的状态：" + current_state_value)
      //      if (current_state_value == null) { //表示该imei还没有被处理过
      //        current_state.update(newlest_event_time)
      //      } else { //表示该imei已经有数据，新数据需要比较
      //        if (newlest_event_time > current_state_value) {
      //          current_state.update(newlest_event_time)
      //          flag = true
      //        }
      //      }
      //      println("      ------> 更新后的状态：" + current_state.value())
      //      flag
      val l: Long = map_state.get(imei + "1")

      print("------------>数据:" + value + "   开始灌入1000000状态")
      for (i <- 1 to 100000) {
        map_state.put(imei + i, newlest_event_time)
      }
      println("----------->end")
      true

    }
  }

}
