package com.lvxian.feature

import com.vivo.vector.{MLUtils, Vector}
import scala.collection.JavaConversions._
import com.vivo.ai.feednews.tf.util.Tools._
import scala.collection.mutable

/**
  * @author gongjingkang
  * @date 2020/6/10 18:00
  *//**
  * @program: feednews_smart_launcher_data_flow
  * @description: ${description}
  * @author: jkgong
  * @create: 2020-06-10 18:00
  */
object FrequencyControlFeature {
  // 最近5分钟
  private val recentlyTime = 5 * 60 * 1000l
  // 长周期12小时
  private val longTime = 12 * 60 * 60 * 1000l
  // 短周期1小时
  private val oneHourTime = 1 * 60 * 60 * 1000l
  // 48小时
  private val forty8HourTime = 48 * 60 * 60 * 1000l
  // 7天
  private val sevenDayTime = 7 * 24 * 60 * 60 * 1000l
  // 14天
  private val fourteenDayTime = 14 * 24 * 60 * 60 * 1000l
  val intervals = Array(recentlyTime, longTime, oneHourTime, forty8HourTime, sevenDayTime, fourteenDayTime)

  def addFrequencyControlFeature(fieldName: String, reqTimestamp: Long, featureName: String, sequenceShowFeature: com.vivo.ai.tuple.Tuple2[Array[Long], Array[String]], sequenceClickFeature: com.vivo.ai.tuple.Tuple2[Array[Long], Array[String]], featureMap: mutable.LinkedHashMap[String, scala.collection.mutable.ArrayBuffer[Any]]): Unit = {
    val Array(recentlyShowTimes, longShowTimes, f1HourShowTimes, f48HourShowTimes, f7DayShowTimes, f14DayShowTimes) =
      MLUtils.statisticsSequenceTimes(sequenceShowFeature, reqTimestamp, featureName,
        Array(recentlyTime, longTime, oneHourTime, forty8HourTime, sevenDayTime, fourteenDayTime))
    featureMap(s"sl_news_${fieldName}_r_s_times") += recentlyShowTimes.toDouble
    featureMap(s"sl_news_${fieldName}_l_s_times") += longShowTimes.toDouble
    featureMap(s"sl_news_${fieldName}_1hour_s_times") += f1HourShowTimes.toDouble
    featureMap(s"sl_news_${fieldName}_48hour_s_times") += f48HourShowTimes.toDouble
    featureMap(s"sl_news_${fieldName}_7days_s_times") += f7DayShowTimes.toDouble
    featureMap(s"sl_news_${fieldName}_14days_s_times") += f14DayShowTimes.toDouble

    val Array(recentlyClickTimes, longClickTimes, f1HourClickTimes, f48HourClickTimes, f7DayClickTimes, f14DayClickTimes) =
      MLUtils.statisticsSequenceTimes(sequenceClickFeature, reqTimestamp, featureName,
        Array(recentlyTime, longTime, oneHourTime, forty8HourTime, sevenDayTime, fourteenDayTime))
    featureMap(s"sl_news_${fieldName}_r_c_times") += recentlyClickTimes.toDouble
    featureMap(s"sl_news_${fieldName}_l_c_times") += longClickTimes.toDouble
    featureMap(s"sl_news_${fieldName}_1hour_c_times") += f1HourClickTimes.toDouble
    featureMap(s"sl_news_${fieldName}_48hour_c_times") += f48HourClickTimes.toDouble
    featureMap(s"sl_news_${fieldName}_7days_c_times") += f7DayClickTimes.toDouble
    featureMap(s"sl_news_${fieldName}_14days_c_times") += f14DayClickTimes.toDouble
  }

  def addMutiFrequencyControlFeature(fieldName: String,
                                     reqTimestamp: Long,
                                     featureNames: List[String],
                                     vectorSize: Int,
                                     sequenceShowFeature: com.vivo.ai.tuple.Tuple2[Array[Long], Array[String]],
                                     sequenceClickFeature: com.vivo.ai.tuple.Tuple2[Array[Long], Array[String]],
                                     featureMap: mutable.LinkedHashMap[String, scala.collection.mutable.ArrayBuffer[Any]]): Unit = {
    val Array(recentlyShowTimes, longShowTimes, f1HourShowTimes, f48HourShowTimes, f7DayShowTimes, f14DayShowTimes) =
      MLUtils2.statisticsSequenceTimes(sequenceShowFeature, reqTimestamp, featureNames,
        Array(recentlyTime, longTime, oneHourTime, forty8HourTime, sevenDayTime, fourteenDayTime), "#")


    featureMap(s"sl_news_${fieldName}_r_s_times_indices") ++= map2Vec(recentlyShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_r_s_times_values") ++= map2Vec(recentlyShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_l_s_times_indices") ++= map2Vec(longShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_l_s_times_values") ++= map2Vec(longShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_1hour_s_times_indices") ++= map2Vec(f1HourShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_1hour_s_times_values") ++= map2Vec(f1HourShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_48hour_s_times_indices") ++= map2Vec(f48HourShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_48hour_s_times_values") ++= map2Vec(f48HourShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_7days_s_times_indices") ++= map2Vec(f7DayShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_7days_s_times_values") ++= map2Vec(f7DayShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_14days_s_times_indices") ++= map2Vec(f14DayShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_14days_s_times_values") ++= map2Vec(f14DayShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)


    val Array(recentlyClickTimes, longClickTimes, f1HourClickTimes, f48HourClickTimes, f7DayClickTimes, f14DayClickTimes) =
      MLUtils2.statisticsSequenceTimes(sequenceClickFeature, reqTimestamp, featureNames,
        Array(recentlyTime, longTime, oneHourTime, forty8HourTime, sevenDayTime, fourteenDayTime), "#")
    featureMap(s"sl_news_${fieldName}_r_c_times_indices") ++= map2Vec(recentlyClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_r_c_times_values") ++= map2Vec(recentlyClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_l_c_times_indices") ++= map2Vec(longClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_l_c_times_values") ++= map2Vec(longClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_1hour_c_times_indices") ++= map2Vec(f1HourClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_1hour_c_times_values") ++= map2Vec(f1HourClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_48hour_c_times_indices") ++= map2Vec(f48HourClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_48hour_c_times_values") ++= map2Vec(f48HourClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_7days_c_times_indices") ++= map2Vec(f7DayClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_7days_c_times_values") ++= map2Vec(f7DayClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_14days_c_times_indices") ++= map2Vec(f14DayClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_14days_c_times_values") ++= map2Vec(f14DayClickTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
  }

  def addDurationFrequencyControlFeature(fieldName: String,
                                         reqTimestamp: Long,
                                         featureName: String,
                                         sequenceDurationFeature: com.vivo.ai.tuple.Tuple3[Array[Long], Array[String], Array[Double]],
                                         featureMap: mutable.LinkedHashMap[String, scala.collection.mutable.ArrayBuffer[Any]]): Unit = {
    val Array(recentlyShowTimes, longShowTimes, f1HourShowTimes, f48HourShowTimes, f7DayShowTimes, f14DayShowTimes) =
      MLUtils2.statisticsDurationSequenceTimes(sequenceDurationFeature, reqTimestamp, featureName,
        Array(recentlyTime, longTime, oneHourTime, forty8HourTime, sevenDayTime, fourteenDayTime))
    featureMap(s"sl_news_${fieldName}_r_read_duration") += recentlyShowTimes.toDouble
    featureMap(s"sl_news_${fieldName}_l_read_duration") += longShowTimes.toDouble
    featureMap(s"sl_news_${fieldName}_1hour_read_duration") += f1HourShowTimes.toDouble
    featureMap(s"sl_news_${fieldName}_48hour_read_duration") += f48HourShowTimes.toDouble
    featureMap(s"sl_news_${fieldName}_7days_read_duration") += f7DayShowTimes.toDouble
    featureMap(s"sl_news_${fieldName}_14days_read_duration") += f14DayShowTimes.toDouble
  }


  def addMutiDurationFrequencyControlFeature(fieldName: String,
                                             reqTimestamp: Long,
                                             featureNames: List[String],
                                             vectorSize: Int,
                                             sequenceDurationFeature: com.vivo.ai.tuple.Tuple3[Array[Long], Array[String], Array[Double]],
                                             featureMap: mutable.LinkedHashMap[String, scala.collection.mutable.ArrayBuffer[Any]]): Unit = {
    val Array(recentlyShowTimes, longShowTimes, f1HourShowTimes, f48HourShowTimes, f7DayShowTimes, f14DayShowTimes) =
      MLUtils2.statisticsMutiDurationSequenceTimes(sequenceDurationFeature, reqTimestamp, featureNames,
        Array(recentlyTime, longTime, oneHourTime, forty8HourTime, sevenDayTime, fourteenDayTime), "#")

    featureMap(s"sl_news_${fieldName}_r_read_duration_indices") ++= map2Vec(recentlyShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_r_read_duration_values") ++= map2Vec(recentlyShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_l_read_duration_indices") ++= map2Vec(longShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_l_read_duration_values") ++= map2Vec(longShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_1hour_read_duration_indices") ++= map2Vec(f1HourShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_1hour_read_duration_values") ++= map2Vec(f1HourShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_48hour_read_duration_indices") ++= map2Vec(f48HourShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_48hour_read_duration_values") ++= map2Vec(f48HourShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_7days_read_duration_indices") ++= map2Vec(f7DayShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_7days_read_duration_values") ++= map2Vec(f7DayShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
    featureMap(s"sl_news_${fieldName}_14days_read_duration_indices") ++= map2Vec(f14DayShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getIndices
    featureMap(s"sl_news_${fieldName}_14days_read_duration_values") ++= map2Vec(f14DayShowTimes.map(x => (x._1.toInt, x._2.toDouble)).toMap, vectorSize).getValues.map(_.toDouble)
  }

}
