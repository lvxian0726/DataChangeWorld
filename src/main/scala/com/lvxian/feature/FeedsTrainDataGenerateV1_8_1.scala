package com.lvxian.feature

import com.alibaba.fastjson.{JSON, JSONObject}
import com.vivo.ai.data.conn.HBaseConnectInfo
import com.vivo.ai.data.hbase.HBaseUtils
import com.vivo.ai.encode.SceneEncoder
import com.vivo.ai.feednews.tf.util.{Hdfs2Rdd_V1_7, RfluxFeature}
import com.vivo.utils.Gzip
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * @author lvxian 72060610
  * @version 2020/11/19 10:32
  */

object FeedsTrainDataGenerateV1_8_1 {
  private def trainDataFromHbase(spark: SparkSession, startMinute: String, endMinute: String, sampling: Double, dataFrom: String) = {
    val startTimestamp = DateUtils.parseDate(startMinute, "yyyy-MM-dd HH mm").getTime
    val endTimestamp = DateUtils.parseDate(endMinute, "yyyy-MM-dd HH mm").getTime
    val day = startMinute.split(" ")(0)
    val hour = startMinute.split(" ")(1)
    val finalData: RDD[(JSONObject, JSONObject)] = dataFrom.toLowerCase match {
      case "hbase" =>
        val hbaseTableName = "ai_feednews:smart_board_feeds"
        val ttl = 259200000l
        val data = HBaseUtils.hbaseResultRDD(spark, hbaseTableName, HBaseConnectInfo.xunyicao, startTimestamp, endTimestamp, ttl).map { result =>
          val token = Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("token")))
          val imei = Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("imei")))
          val dt = Bytes.toLong(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("dt")))
          val eventTime = Bytes.toLong(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("req_timestamp")))
          val event_id = Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("event_id")))
          val timeGap = eventTime - token.substring(0, 13).toLong

          val continuous_clicked = try {
            Bytes.toDouble(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("continuous_clicked")))
          } catch {
            case e: Exception => 0.0
          }

          val continuous_request = try {
            Bytes.toDouble(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("continuous_request")))
          } catch {
            case e: Exception => 0.0
          }

          val userFeature = try {
            val userFeatureJson = JSON.parseObject(Bytes.toString(Gzip.uncompress(result.getValue(Bytes.toBytes("f"), Bytes.toBytes(s"userFeature")))))
            val reluxFeatureJson = JSON.parseObject(Bytes.toString(Gzip.uncompress(result.getValue(Bytes.toBytes("f"), Bytes.toBytes(s"reluxFeature")))))
            if (reluxFeatureJson != null) {
              for (item <- (RfluxFeature.reluxTupleFeatures ++ RfluxFeature.reluxVectorFeatures ++ RfluxFeature.reluxMapFeatures)) {
                if (reluxFeatureJson.get(item) != null) {
                  userFeatureJson.put(item, reluxFeatureJson.get(item))
                }
              }
            }
            userFeatureJson.put("token", token)
            userFeatureJson.put("user_imei", imei)
            userFeatureJson.put("event_id", event_id)
            userFeatureJson.put("event_time", eventTime.toLong)
            userFeatureJson.put("is_holiday", SceneEncoder.isHolidayEncode(eventTime.toLong).toInt)
            userFeatureJson.put("yestoday_is_holiday", SceneEncoder.isHolidayEncode(eventTime.toLong - (24 * 60 * 60 * 1000)).toInt)
            userFeatureJson.put("tomorrow_is_holiday", SceneEncoder.isHolidayEncode(eventTime.toLong + (24 * 60 * 60 * 1000)).toInt)

            userFeatureJson.put("hour_scope", SceneEncoder.getHourEncode(eventTime.toLong).toInt)
            userFeatureJson.put("next_is_holiday", SceneEncoder.nextDayIsHoliday(eventTime.toLong).toInt)
            userFeatureJson.put("week_of_day", SceneEncoder.getWeekOfDayEncode(eventTime.toLong).toInt)
            //          userFeature.put("userRealTimeFeature",userRealTimeFeature)
            userFeatureJson.put("time_gap", timeGap)

            userFeatureJson.put("continuous_clicked", continuous_clicked) //添加连续点击和连续请求特征
            userFeatureJson.put("continuous_request", continuous_request)

            userFeatureJson
          } catch {
            case e: Exception =>
              new JSONObject()
          }
          val cells: mutable.Buffer[String] = result.listCells().map { cell =>
            Bytes.toString(CellUtil.cloneQualifier(cell))
          }
          val newsIds: mutable.Buffer[String] = cells.filter(key => key.startsWith("itemFeature")).map(_.split("_", 2)(1))

          //负反馈
          val newsIds_feedback_negative: mutable.Buffer[String] = cells.filter(key => key.startsWith("feedback_negative")).map(_.split(":", 2)(1))
          //正反馈-点赞
          val newsIds_feedback_like: mutable.Buffer[String] = cells.filter(key => key.startsWith("feedback_like")).map(_.split(":", 2)(1))
          //正反馈-评论
          val newsIds_feedback_comment: mutable.Buffer[String] = cells.filter(key => key.startsWith("feedback_comment")).map(_.split(":", 2)(1))
          //正反馈-收藏
          val newsIds_feedback_collect: mutable.Buffer[String] = cells.filter(key => key.startsWith("feedback_collect")).map(_.split(":", 2)(1))
          //正反馈-分享
          val newsIds_feedback_share: mutable.Buffer[String] = cells.filter(key => key.startsWith("feedback_share")).map(_.split(":", 2)(1))

          val recommendResultMap: Map[String, JSONObject] = newsIds.map { newsId =>
            val recommendResult: Array[Byte] = result.getValue(Bytes.toBytes("f"), Bytes.toBytes(s"recommend_result_${newsId}"))
            (newsId, recommendResult)
          }.filter(x => x._2 != null).map { x =>
            val json: JSONObject = JSON.parseObject(Bytes.toString(x._2))
            (x._1, json)
          }.toMap
          val itemFeatures: mutable.Buffer[JSONObject] = newsIds.map { newsId =>
            val jsonObj: JSONObject = try {
              val jsonStr: String = Bytes.toString(Gzip.uncompress(result.getValue(Bytes.toBytes("f"), Bytes.toBytes(s"itemFeature_${newsId}"))))
              val json: JSONObject = JSON.parseObject(jsonStr)

              try {
                val reflux_itemFeature_Str: String = Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes(s"reflux_itemFeature_${newsId}")))
                val reflux_itemFeature: JSONObject = JSON.parseObject(reflux_itemFeature_Str)
                if (reflux_itemFeature_Str != null && reflux_itemFeature != null) {
                  for (item <- RfluxFeature.reluxItemFeatures) {
                    if (reflux_itemFeature.contains(item) && reflux_itemFeature.get(item).toString.length > 0) {
                      json.put(item, reflux_itemFeature.get(item))
                    }
                  }
                }

              } catch {
                case e: Exception =>
              }

              val durationResult: Array[Byte] = result.getValue(Bytes.toBytes("f"), Bytes.toBytes(s"duration_${newsId}"))
              if (durationResult != null) {
                val duration: Int = Bytes.toInt(durationResult)
                json.put("duration", duration)
              }
              val clicked: Double = {
                val bytes: Array[Byte] = result.getValue(Bytes.toBytes("f"), Bytes.toBytes(s"clk_${newsId}"))
                if (bytes == null) {
                  0.0
                } else {
                  Bytes.toDouble(bytes)
                }
              }
              json.put("clicked", clicked)
              val pctr: Double = {
                if (recommendResultMap.contains(newsId)) {
                  recommendResultMap(newsId).getString("pctr").toDouble
                } else {
                  0.0
                }
              }
              val recall_type: String = {
                if (recommendResultMap.contains(newsId)) {
                  recommendResultMap(newsId).getString("recall_type")
                } else {
                  ""
                }
              }

              val feedback_negative_flag: Int = if (newsIds_feedback_negative.contains(newsId)) 1 else 0
              val feedback_like_flag: Int = if (newsIds_feedback_like.contains(newsId)) 1 else 0
              val feedback_comment_flag: Int = if (newsIds_feedback_comment.contains(newsId)) 1 else 0
              val feedback_collect_flag: Int = if (newsIds_feedback_collect.contains(newsId)) 1 else 0
              val feedback_share_flag: Int = if (newsIds_feedback_share.contains(newsId)) 1 else 0

              json.put("recall_type", recall_type)
              json.put("pctr", pctr)
              json.put("newsId", newsId)
              json.put("feedback_negative_flag", feedback_negative_flag)
              json.put("feedback_like_flag", feedback_like_flag)
              json.put("feedback_comment_flag", feedback_comment_flag)
              json.put("feedback_collect_flag", feedback_collect_flag)
              json.put("feedback_share_flag", feedback_share_flag)
              json
            } catch {
              case e: Exception =>
                new JSONObject()
            }
            jsonObj
          }
          val bizType: String = try {
            recommendResultMap.toArray.take(1)(0)._2.getString("bizType")
          } catch {
            case e: Exception =>
              ""
          }
          (token, imei, dt, newsIds, userFeature, itemFeatures, bizType)
        }.filter(x => x._2 != null && x._3 != null && x._5 != null && x._6 != null && x._7 == "RECOMMEND").map(x => (x._1, x._2, x._3, x._4, x._5, x._6))
        val resultData: RDD[(JSONObject, JSONObject)] = data.flatMap {
          case (token, imei, dt, newsIds, userFeature, itemFeatures) =>
            itemFeatures.toArray.map { itemFeature =>
              (userFeature, itemFeature)
            }
        }.filter(x => x._1 != null && x._2 != null && !x._1.isEmpty && !x._2.isEmpty)
        resultData
      case "hdfs" =>
        val hdfsPath = s"hdfs://bj04-region05/region05/118/app/product/feednews/data/hbase_history/hbase_history/$day/$hour"
        println("本次执行读取的hdfs dir:" + hdfsPath)
        val rdd = Hdfs2Rdd_V1_7.getRDDFromHdfs(hdfsPath, spark)
        val data = rdd.filter(x => x._2 != null && x._3 != null && x._5 != null && x._6 != null && x._7 == "RECOMMEND").map(x => (x._1, x._2, x._3, x._4, x._5, x._6))
        val resultData = data.flatMap {
          case (token, imei, dt, newsIds, userFeature, itemFeatures) =>
            itemFeatures.toArray.map { itemFeature =>
              (userFeature, itemFeature)
            }
        }.filter(x => x._1 != null && x._2 != null && !x._1.isEmpty && !x._2.isEmpty)
        resultData
    }

    //    finalData
    finalData.filter(x => (x._1.getLong("event_time") - x._1.getString("token").substring(0, 13).toLong) > 0 && (x._1.getLong("event_time") - x._1.getString("token").substring(0, 13).toLong) < 3600000)
      .filter(x => x._2.getLong("collect_start_timestamp") != null && (x._2.getLong("collect_start_timestamp") > startTimestamp))
      .filter(x => !x._2.getString("newsId").startsWith("V06") && !x._2.getString("recall_type").startsWith("25")) //过滤掉人民网(置顶)，25开头的召回方式都是热点相关的
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("FeedsTrainDataGenerateV1_7_4_test").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    val Array(dataFrom, startMinute, endMinute, sampling, filePath) = args
    //        val Array(dataFrom, startMinute, endMinute, sampling, filePath) = Array("hbase", "2019-11-27 01 00", "2019-11-27 02 00", "1.0", "hbase")
    val rdd: RDD[(JSONObject, JSONObject)] = trainDataFromHbase(spark, startMinute, endMinute, sampling.toDouble, dataFrom)
    val tmpRDD: RDD[(JSONObject, JSONObject)] = sc.parallelize(rdd.take(100)).cache()
    println("本次执行从" + dataFrom + "读取到数据量：" + tmpRDD.count())
    tmpRDD.take(5).foreach(value => {
      println("userFeature数据:")
      println(value._1)
      println("itemFeature数据:")
      println(value._2)
    })
    val repartition = math.ceil(tmpRDD.count().toDouble / 100000).toInt
    FeedsTFRProcessorV1_8.toTFrecord(spark, tmpRDD, 1, filePath, repartition)
    sc.stop()

  }
}

