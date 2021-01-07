package com.lvxian.feature

import com.alibaba.fastjson.{JSON, JSONObject}
import com.vivo.ai.data.util.IOUtils
import com.vivo.ai.encode.util.EncodeEnv
import com.vivo.ai.encode.{ContinuousEncoder, SceneEncoder}
import com.vivo.ai.feednews.streaming.utils.CTR
import com.vivo.ai.feednews.tf.util.Tools._
import com.vivo.ai.feednews.tf.util.{FrequencyControlFeature, Tools}
import com.vivo.ai.tuple
import com.vivo.vector.{MLUtils, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object FeedsTFRProcessorV1_8 {
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
  private val defaultTopicVec = Vector.builder(2001).index(Array(2000)).value(Array(0.0F)).build()
  val intervals = Array(recentlyTime, longTime, oneHourTime, forty8HourTime, sevenDayTime, fourteenDayTime)


  private val feature = IOUtils.fromResource("feature/smart_board_featuresV1_8.txt").map(_.split("\\s+", 2)).filter(x => x.length == 2).map(arr => (arr(0), arr(1)))

  def handleBatch(batch: Seq[(JSONObject, JSONObject)]): Row = {
    val featureMap = new mutable.LinkedHashMap[String, scala.collection.mutable.ArrayBuffer[Any]]
    feature.map { x => featureMap.put(x._1, new ArrayBuffer[Any]()) }
    var sparseVectorNum = 0
    batch.foreach {
      case (userJSONObject, itemJSONObject) =>
        val userRealTimeFeature = userJSONObject.getJSONObject("userRealTimeFeature")
        val clicked = itemJSONObject.getDouble("clicked")
        val token = userJSONObject.getString("token")
        val reqTimestamp = token.substring(0, 13).toLong
        val eventTime = userJSONObject.getString("event_time").toLong
        featureMap("rowkey") += userJSONObject.getString("rowkey")

        featureMap("event_id") += userJSONObject.getString("event_id")
        featureMap("token") += userJSONObject.getString("token")
        //category特征
        featureMap("experiment_rank_layer") += getExperimentID(userJSONObject.getString("user_imei"), "level-9970-4", 100)
        featureMap("group_id") += getExperimentID(userJSONObject.getString("user_imei"), null, 10000)
        featureMap("clicked") += clicked.toInt
        featureMap("user_imei") += userJSONObject.getString("user_imei")
        featureMap("user_imei_hash") += userJSONObject.getString("user_imei").hashCode
        featureMap("user_imei_hash_mod") += (userJSONObject.getString("user_imei").hashCode % 10000000).abs
        featureMap("user_sex") += userJSONObject.getIntValue("user_sex")
        featureMap("user_age") += userJSONObject.getIntValue("user_age")
        featureMap("user_game_pay") += userJSONObject.getIntValue("user_game_pay")
        featureMap("user_browser_version") += userJSONObject.getIntValue("user_browser_version")
        featureMap("user_browser_act_level") += userJSONObject.getIntValue("user_browser_act_level")
        featureMap("user_appstore_act_level") += userJSONObject.getIntValue("user_appstore_act_level")
        featureMap("user_gc_act_level") += userJSONObject.getIntValue("user_gc_act_level")
        featureMap("user_sim1") += userJSONObject.getIntValue("user_sim1")
        featureMap("user_sim2") += userJSONObject.getIntValue("user_sim2")
        featureMap("user_mod_main_type") += userJSONObject.getIntValue("user_mod_main_type")
        featureMap("user_mod_child_type") += userJSONObject.getIntValue("user_mod_child_type")
        featureMap("user_mod_series") += userJSONObject.getIntValue("user_mod_series")
        featureMap("user_mod_main_color") += userJSONObject.getIntValue("user_mod_main_color")
        featureMap("user_mod_child_color") += userJSONObject.getIntValue("user_mod_child_color")
        featureMap("user_res_city") += userJSONObject.getIntValue("user_res_city")
        val userResCityName = userJSONObject.getString("user_res_city_name")
        featureMap("user_res_city_grade") += userJSONObject.getIntValue("user_res_city_grade")
        featureMap("user_res_prvn") += userJSONObject.getIntValue("user_res_prvn")
        featureMap("user_res_prvn_capital") += userJSONObject.getIntValue("user_res_prvn_capital")
        featureMap("user_res_region") += userJSONObject.getIntValue("user_res_region")
        val userSaleDate = userJSONObject.getLongValue("user_sale_date")
        val userSaleDuration = (eventTime - userSaleDate) / (24 * 60 * 60 * 1000L * 30)
        val userSaleMonthDuration = if (userSaleDuration >= 60) 60 else userSaleDuration
        featureMap("user_sale_month") += userSaleMonthDuration.toDouble
        val userPositiveNewsTopCategoryV3 = userJSONObject.getObject("user_positive_news_top_category_v3", classOf[Array[Int]])
        val userNewsLongTag = if (Tools.jsonGetMap(userJSONObject, "user_news_long_tag").isEmpty) Map(0 -> 0.0) else Tools.jsonGetMap(userJSONObject, "user_news_long_tag")
        val userNewsLongTagHashArr = userNewsLongTag.toArray.map { x => (x._1.hashCode, x._2) }.groupBy(_._1).mapValues(_.map(_._2).sum).toArray.sortBy(_._1)
        val userNewsLongTagVec = Vector.builder(500000).index(userNewsLongTagHashArr.map(_._1)).value(userNewsLongTagHashArr.map(_._2.toFloat)).build()
        val (userNewsLongTagVecIndices, userNewsLongTagVecValues) = vec2TFSparseVec(userNewsLongTagVec)
        featureMap("user_news_long_tag_vec_indices") ++= userNewsLongTagVecIndices
        featureMap("user_news_long_tag_vec_values") ++= userNewsLongTagVecValues


        val (userDislikeNewsTopCategoryV3Indices, userDislikeNewsTopCategoryV3Values) = vec2TFSparseVec(userJSONObject.getObject("user_dislike_news_top_category_v3", classOf[Vector]))
        featureMap("user_dislike_news_top_category_v3_indices") ++= userDislikeNewsTopCategoryV3Indices
        featureMap("user_dislike_news_top_category_v3_values") ++= userDislikeNewsTopCategoryV3Values
        val (userDislikeNewsSubCategoryV3Indices, userDislikeNewsSubCategoryV3Values) = vec2TFSparseVec(userJSONObject.getObject("user_dislike_news_sub_category_v3", classOf[Vector]))
        featureMap("user_dislike_news_sub_category_v3_indices") ++= userDislikeNewsSubCategoryV3Indices
        featureMap("user_dislike_news_sub_category_v3_values") ++= userDislikeNewsSubCategoryV3Values
        //numerical特征
        featureMap("user_game_cate") ++= userJSONObject.getObject("user_game_cate", classOf[Vector]).toDense.getValues.map(_.toDouble)
        featureMap("user_browser_act_days") += userJSONObject.getDoubleValue("user_browser_act_days")
        featureMap("user_appstore_act_days") += userJSONObject.getDoubleValue("user_appstore_act_days")
        featureMap("user_gc_act_days") += userJSONObject.getDoubleValue("user_gc_act_days")
        val (userAdClickTypeIndices, userAdClickTypeValues) = vec2TFSparseVec(userJSONObject.getObject("user_ad_click_type", classOf[Vector]))
        featureMap("user_ad_click_type_indices") ++= userAdClickTypeIndices
        featureMap("user_ad_click_type_values") ++= userAdClickTypeValues
        val (userPreferenceIndices, userPreferenceValues) = vec2TFSparseVec(userJSONObject.getObject("user_preference", classOf[Vector]))
        featureMap("user_preference_indices") ++= userPreferenceIndices
        featureMap("user_preference_values") ++= userPreferenceValues
        val (userAppFtypeUseIndices, userAppFtypeUseValues) = vec2TFSparseVec(userJSONObject.getObject("user_app_ftype_use", classOf[Vector]))
        featureMap("user_app_ftype_use_indices") ++= userAppFtypeUseIndices
        featureMap("user_app_ftype_use_values") ++= userAppFtypeUseValues
        val (userAppLabelsUseIndices, userAppLabelsUseValues) = vec2TFSparseVec(userJSONObject.getObject("user_app_labels_use", classOf[Vector]))
        featureMap("user_app_labels_use_indices") ++= userAppLabelsUseIndices
        featureMap("user_app_labels_use_values") ++= userAppLabelsUseValues
        val (userVisitNewsChannelIndices, userVisitNewsChannelValues) = vec2TFSparseVec(userJSONObject.getObject("user_visit_news_channel", classOf[Vector]))
        featureMap("user_visit_news_channel_indices") ++= userVisitNewsChannelIndices
        featureMap("user_visit_news_channel_values") ++= userVisitNewsChannelValues
        val (userReadNewsCategoryIndices, userReadNewsCategoryValues) = vec2TFSparseVec(userJSONObject.getObject("user_read_news_category", classOf[Vector]))
        featureMap("user_read_news_category_indices") ++= userReadNewsCategoryIndices
        featureMap("user_read_news_category_values") ++= userReadNewsCategoryValues

        val (userReadNewsSubCategoryIndices, userReadNewsSubCategoryValues) = vec2TFSparseVec(userJSONObject.getObject("user_read_news_sub_category", classOf[Vector]))
        featureMap("user_read_news_sub_category_indices") ++= userReadNewsSubCategoryIndices
        featureMap("user_read_news_sub_category_values") ++= userReadNewsSubCategoryValues
        val (userReadNewsTriCategoryIndices, userReadNewsTriCategoryValues) = vec2TFSparseVec(userJSONObject.getObject("user_read_news_tri_category", classOf[Vector]))
        featureMap("user_read_news_tri_category_indices") ++= userReadNewsTriCategoryIndices
        featureMap("user_read_news_tri_category_values") ++= userReadNewsTriCategoryValues
        val (userNewsLongTopicIndices, userNewsLongTopicValues) = Tools.vec2TFSparseVec(userJSONObject.getObject("user_news_long_topic", classOf[Vector]), defaultTopicVec)
        featureMap("user_news_long_topic_indices") ++= userNewsLongTopicIndices
        featureMap("user_news_long_topic_values") ++= userNewsLongTopicValues
        val (userNewsShortTopicIndices, userNewsShortTopicValues) = Tools.vec2TFSparseVec(userJSONObject.getObject("user_news_short_topic", classOf[Vector]), defaultTopicVec)
        featureMap("user_news_short_topic_indices") ++= userNewsShortTopicIndices
        featureMap("user_news_short_topic_values") ++= userNewsShortTopicValues

        //*****************************json格式新序列相关特征 2020-12-10*****************************
        //show
        val userSL2FeedNewsShowFcNewsId: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_id", userJSONObject, "string").getT2.map(_.hashCode)
        val userSL2FeedNewsShowFcNewsIdFill: Array[Int] = userSL2FeedNewsShowFcNewsId ++ new Array[Int](121 - userSL2FeedNewsShowFcNewsId.length)
        featureMap("user_sl2_feed_news_show_fc_news_id") ++= userSL2FeedNewsShowFcNewsIdFill
        featureMap("user_sl2_feed_news_show_fc_news_id_length") += userSL2FeedNewsShowFcNewsId.length

        val userSL2FeedNewsShowFcNewsTopCategoryV3Update: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "top_category_v3_update", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsShowFcNewsTopCategoryV3UpdateFill: Array[Int] = userSL2FeedNewsShowFcNewsTopCategoryV3Update ++ new Array[Int](121 - userSL2FeedNewsShowFcNewsTopCategoryV3Update.length)
        featureMap("user_sl2_feed_news_show_fc_news_top_category_v3_update") ++= userSL2FeedNewsShowFcNewsTopCategoryV3UpdateFill
        featureMap("user_sl2_feed_news_show_fc_news_top_category_v3_update_length") += userSL2FeedNewsShowFcNewsTopCategoryV3Update.length

        val userSL2FeedNewsShowFcNewsSubCategoryV3Update: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "sub_category_v3_update", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsShowFcNewsSubCategoryV3UpdateFill: Array[Int] = userSL2FeedNewsShowFcNewsSubCategoryV3Update ++ new Array[Int](121 - userSL2FeedNewsShowFcNewsSubCategoryV3Update.length)
        featureMap("user_sl2_feed_news_show_fc_news_sub_category_v3_update") ++= userSL2FeedNewsShowFcNewsSubCategoryV3UpdateFill
        featureMap("user_sl2_feed_news_show_fc_news_sub_category_v3_update_length") += userSL2FeedNewsShowFcNewsSubCategoryV3Update.length

        val userSL2FeedNewsShowFcNewsThirdCategoryV3Update: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "third_category_v3_update", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsShowFcNewsThirdCategoryV3UpdateFill: Array[Int] = userSL2FeedNewsShowFcNewsThirdCategoryV3Update ++ new Array[Int](121 - userSL2FeedNewsShowFcNewsThirdCategoryV3Update.length)
        featureMap("user_sl2_feed_news_show_fc_news_third_category_v3_update") ++= userSL2FeedNewsShowFcNewsThirdCategoryV3UpdateFill
        featureMap("user_sl2_feed_news_show_fc_news_third_category_v3_update_length") += userSL2FeedNewsShowFcNewsThirdCategoryV3Update.length

        val userSL2FeedNewsShowFcNewsTopCategoryV3: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_category_v3", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsShowFcNewsTopCategoryV3Fill: Array[Int] = userSL2FeedNewsShowFcNewsTopCategoryV3 ++ new Array[Int](121 - userSL2FeedNewsShowFcNewsTopCategoryV3.length)
        featureMap("user_sl2_feed_news_show_fc_news_top_category_v3") ++= userSL2FeedNewsShowFcNewsTopCategoryV3Fill
        featureMap("user_sl2_feed_news_show_fc_news_top_category_v3_length") += userSL2FeedNewsShowFcNewsTopCategoryV3.length

        val userSL2FeedNewsShowFcNewsSubCategoryV3: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_subcategory_v3", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsShowFcNewsSubCategoryV3Fill: Array[Int] = userSL2FeedNewsShowFcNewsSubCategoryV3 ++ new Array[Int](121 - userSL2FeedNewsShowFcNewsSubCategoryV3.length)
        featureMap("user_sl2_feed_news_show_fc_news_sub_category_v3") ++= userSL2FeedNewsShowFcNewsSubCategoryV3Fill
        featureMap("user_sl2_feed_news_show_fc_news_sub_category_v3_length") += userSL2FeedNewsShowFcNewsSubCategoryV3.length

        val userSL2FeedNewsShowFcNewsContentTagV2Top5: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_content_tag_vec_v2", userJSONObject, "json", 5).getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSL2FeedNewsShowFcNewsContentTagV2Top5Fill: Array[Int] = userSL2FeedNewsShowFcNewsContentTagV2Top5 ++ new Array[Int](1000 - userSL2FeedNewsShowFcNewsContentTagV2Top5.length)
        featureMap("user_sl2_feed_news_show_fc_news_content_tag_v2_top5") ++= userSL2FeedNewsShowFcNewsContentTagV2Top5Fill
        featureMap("user_sl2_feed_news_show_fc_news_content_tag_v2_top5_length") += userSL2FeedNewsShowFcNewsContentTagV2Top5.length

        val userSL2FeedNewsShowFcNewsTitleTagV2Top5: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_original_title_tag_v2", userJSONObject, "json", 5).getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSL2FeedNewsShowFcNewsTitleTagV2Top5Fill: Array[Int] = userSL2FeedNewsShowFcNewsTitleTagV2Top5 ++ new Array[Int](1000 - userSL2FeedNewsShowFcNewsTitleTagV2Top5.length)
        featureMap("user_sl2_feed_news_show_fc_news_title_tag_v2_top5") ++= userSL2FeedNewsShowFcNewsTitleTagV2Top5Fill
        featureMap("user_sl2_feed_news_show_fc_news_title_tag_v2_top5_length") += userSL2FeedNewsShowFcNewsTitleTagV2Top5.length

        val userSL2FeedNewsShowFcNewsTopicV2Top5: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_topic_vec_v2", userJSONObject, "json", 5).getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSL2FeedNewsShowFcNewsTopicV2Top5Fill: Array[Int] = userSL2FeedNewsShowFcNewsTopicV2Top5 ++ new Array[Int](1000 - userSL2FeedNewsShowFcNewsTopicV2Top5.length)
        featureMap("user_sl2_feed_news_show_fc_news_topic_v2_top5") ++= userSL2FeedNewsShowFcNewsTopicV2Top5Fill
        featureMap("user_sl2_feed_news_show_fc_news_topic_v2_top5_length") += userSL2FeedNewsShowFcNewsTopicV2Top5.length

        //click
        val userSL2FeedNewsClickFcNewsId: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_id", userJSONObject, "string").getT2.map(_.hashCode)
        val userSL2FeedNewsClickFcNewsIdFill: Array[Int] = userSL2FeedNewsClickFcNewsId ++ new Array[Int](121 - userSL2FeedNewsClickFcNewsId.length)
        featureMap("user_sl2_feed_news_click_fc_news_id") ++= userSL2FeedNewsClickFcNewsIdFill
        featureMap("user_sl2_feed_news_click_fc_news_id_length") += userSL2FeedNewsClickFcNewsId.length

        val userSL2FeedNewsClickFcNewsTopCategoryV3Update: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "top_category_v3_update", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsClickFcNewsTopCategoryV3UpdateFill: Array[Int] = userSL2FeedNewsClickFcNewsTopCategoryV3Update ++ new Array[Int](121 - userSL2FeedNewsClickFcNewsTopCategoryV3Update.length)
        featureMap("user_sl2_feed_news_click_fc_news_top_category_v3_update") ++= userSL2FeedNewsClickFcNewsTopCategoryV3UpdateFill
        featureMap("user_sl2_feed_news_click_fc_news_top_category_v3_update_length") += userSL2FeedNewsClickFcNewsTopCategoryV3Update.length

        val userSL2FeedNewsClickFcNewsSubCategoryV3Update: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "sub_category_v3_update", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsClickFcNewsSubCategoryV3UpdateFill: Array[Int] = userSL2FeedNewsClickFcNewsSubCategoryV3Update ++ new Array[Int](121 - userSL2FeedNewsClickFcNewsSubCategoryV3Update.length)
        featureMap("user_sl2_feed_news_click_fc_news_sub_category_v3_update") ++= userSL2FeedNewsClickFcNewsSubCategoryV3UpdateFill
        featureMap("user_sl2_feed_news_click_fc_news_sub_category_v3_update_length") += userSL2FeedNewsClickFcNewsSubCategoryV3Update.length

        val userSL2FeedNewsClickFcNewsThirdCategoryV3Update: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "third_category_v3_update", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsClickFcNewsThirdCategoryV3UpdateFill: Array[Int] = userSL2FeedNewsClickFcNewsThirdCategoryV3Update ++ new Array[Int](121 - userSL2FeedNewsClickFcNewsThirdCategoryV3Update.length)
        featureMap("user_sl2_feed_news_click_fc_news_third_category_v3_update") ++= userSL2FeedNewsClickFcNewsThirdCategoryV3UpdateFill
        featureMap("user_sl2_feed_news_click_fc_news_third_category_v3_update_length") += userSL2FeedNewsClickFcNewsThirdCategoryV3Update.length

        val userSL2FeedNewsClickFcNewsTopCategoryV3: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_category_v3", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsClickFcNewsTopCategoryV3Fill: Array[Int] = userSL2FeedNewsClickFcNewsTopCategoryV3 ++ new Array[Int](121 - userSL2FeedNewsClickFcNewsTopCategoryV3.length)
        featureMap("user_sl2_feed_news_click_fc_news_top_category_v3") ++= userSL2FeedNewsClickFcNewsTopCategoryV3Fill
        featureMap("user_sl2_feed_news_click_fc_news_top_category_v3_length") += userSL2FeedNewsClickFcNewsTopCategoryV3.length

        val userSL2FeedNewsClickFcNewsSubCategoryV3: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_subcategory_v3", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsClickFcNewsSubCategoryV3Fill: Array[Int] = userSL2FeedNewsClickFcNewsSubCategoryV3 ++ new Array[Int](121 - userSL2FeedNewsClickFcNewsSubCategoryV3.length)
        featureMap("user_sl2_feed_news_click_fc_news_sub_category_v3") ++= userSL2FeedNewsClickFcNewsSubCategoryV3Fill
        featureMap("user_sl2_feed_news_click_fc_news_sub_category_v3_length") += userSL2FeedNewsClickFcNewsSubCategoryV3.length

        val userSL2FeedNewsClickFcNewsContentTagV2Top5: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_content_tag_vec_v2", userJSONObject, "json", 5).getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSL2FeedNewsClickFcNewsContentTagV2Top5Fill: Array[Int] = userSL2FeedNewsClickFcNewsContentTagV2Top5 ++ new Array[Int](1000 - userSL2FeedNewsClickFcNewsContentTagV2Top5.length)
        featureMap("user_sl2_feed_news_click_fc_news_content_tag_v2_top5") ++= userSL2FeedNewsClickFcNewsContentTagV2Top5Fill
        featureMap("user_sl2_feed_news_click_fc_news_content_tag_v2_top5_length") += userSL2FeedNewsClickFcNewsContentTagV2Top5.length

        val userSL2FeedNewsClickFcNewsTitleTagV2Top5: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_original_title_tag_v2", userJSONObject, "json", 5).getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSL2FeedNewsClickFcNewsTitleTagV2Top5Fill: Array[Int] = userSL2FeedNewsClickFcNewsTitleTagV2Top5 ++ new Array[Int](1000 - userSL2FeedNewsClickFcNewsTitleTagV2Top5.length)
        featureMap("user_sl2_feed_news_click_fc_news_title_tag_v2_top5") ++= userSL2FeedNewsClickFcNewsTitleTagV2Top5Fill
        featureMap("user_sl2_feed_news_click_fc_news_title_tag_v2_top5_length") += userSL2FeedNewsClickFcNewsTitleTagV2Top5.length

        val userSL2FeedNewsClickFcNewsTopicV2Top5: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_topic_vec_v2", userJSONObject, "json", 5).getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSL2FeedNewsClickFcNewsTopicV2Top5Fill: Array[Int] = userSL2FeedNewsClickFcNewsTopicV2Top5 ++ new Array[Int](1000 - userSL2FeedNewsClickFcNewsTopicV2Top5.length)
        featureMap("user_sl2_feed_news_click_fc_news_topic_v2_top5") ++= userSL2FeedNewsClickFcNewsTopicV2Top5Fill
        featureMap("user_sl2_feed_news_click_fc_news_topic_v2_top5_length") += userSL2FeedNewsClickFcNewsTopicV2Top5.length


        //duration
        val userSL2FeedNewsDurationFcNewsId: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_duration_fc_v2", "news_id", userJSONObject, "string").getT2.map(_.hashCode)
        val userSL2FeedNewsDurationFcNewsIdFill: Array[Int] = userSL2FeedNewsDurationFcNewsId ++ new Array[Int](121 - userSL2FeedNewsDurationFcNewsId.length)
        featureMap("user_sl2_feed_news_duration_fc_news_id") ++= userSL2FeedNewsDurationFcNewsIdFill
        featureMap("user_sl2_feed_news_duration_fc_news_id_length") += userSL2FeedNewsDurationFcNewsId.length

        val userSL2FeedNewsDurationFcNewsTopCategoryV3Update: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_duration_fc_v2", "top_category_v3_update", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsDurationFcNewsTopCategoryV3UpdateFill: Array[Int] = userSL2FeedNewsDurationFcNewsTopCategoryV3Update ++ new Array[Int](121 - userSL2FeedNewsDurationFcNewsTopCategoryV3Update.length)
        featureMap("user_sl2_feed_news_duration_fc_news_top_category_v3_update") ++= userSL2FeedNewsDurationFcNewsTopCategoryV3UpdateFill
        featureMap("user_sl2_feed_news_duration_fc_news_top_category_v3_update_length") += userSL2FeedNewsDurationFcNewsTopCategoryV3Update.length

        val userSL2FeedNewsDurationFcNewsSubCategoryV3Update: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_duration_fc_v2", "sub_category_v3_update", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsDurationFcNewsSubCategoryV3UpdateFill: Array[Int] = userSL2FeedNewsDurationFcNewsSubCategoryV3Update ++ new Array[Int](121 - userSL2FeedNewsDurationFcNewsSubCategoryV3Update.length)
        featureMap("user_sl2_feed_news_duration_fc_news_sub_category_v3_update") ++= userSL2FeedNewsDurationFcNewsSubCategoryV3UpdateFill
        featureMap("user_sl2_feed_news_duration_fc_news_sub_category_v3_update_length") += userSL2FeedNewsDurationFcNewsSubCategoryV3Update.length


        val userSL2FeedNewsDurationFcNewsThirdCategoryV3Update: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_duration_fc_v2", "third_category_v3_update", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsDurationFcNewsThirdCategoryV3UpdateFill: Array[Int] = userSL2FeedNewsDurationFcNewsThirdCategoryV3Update ++ new Array[Int](121 - userSL2FeedNewsDurationFcNewsThirdCategoryV3Update.length)
        featureMap("user_sl2_feed_news_duration_fc_news_third_category_v3_update") ++= userSL2FeedNewsDurationFcNewsThirdCategoryV3UpdateFill
        featureMap("user_sl2_feed_news_duration_fc_news_third_category_v3_update_length") += userSL2FeedNewsDurationFcNewsThirdCategoryV3Update.length


        val userSL2FeedNewsDurationFcNewsTopCategoryV3: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_duration_fc_v2", "news_category_v3", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsDurationFcNewsTopCategoryV3Fill: Array[Int] = userSL2FeedNewsDurationFcNewsTopCategoryV3 ++ new Array[Int](121 - userSL2FeedNewsDurationFcNewsTopCategoryV3.length)
        featureMap("user_sl2_feed_news_duration_fc_news_top_category_v3") ++= userSL2FeedNewsDurationFcNewsTopCategoryV3Fill
        featureMap("user_sl2_feed_news_duration_fc_news_top_category_v3_length") += userSL2FeedNewsDurationFcNewsTopCategoryV3.length

        val userSL2FeedNewsDurationFcNewsSubCategoryV3: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_duration_fc_v2", "news_subcategory_v3", userJSONObject, "json").getT2.map(_.toInt)
        val userSL2FeedNewsDurationFcNewsSubCategoryV3Fill: Array[Int] = userSL2FeedNewsDurationFcNewsSubCategoryV3 ++ new Array[Int](121 - userSL2FeedNewsDurationFcNewsSubCategoryV3.length)
        featureMap("user_sl2_feed_news_duration_fc_news_sub_category_v3") ++= userSL2FeedNewsDurationFcNewsSubCategoryV3Fill
        featureMap("user_sl2_feed_news_duration_fc_news_sub_category_v3_length") += userSL2FeedNewsDurationFcNewsSubCategoryV3.length

        val userSL2FeedNewsDurationFcNewsContentTagV2Top5: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_duration_fc_v2", "news_content_tag_vec_v2", userJSONObject, "json", 5).getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSL2FeedNewsDurationFcNewsContentTagV2Top5Fill: Array[Int] = userSL2FeedNewsDurationFcNewsContentTagV2Top5 ++ new Array[Int](1000 - userSL2FeedNewsDurationFcNewsContentTagV2Top5.length)
        featureMap("user_sl2_feed_news_duration_fc_news_content_tag_v2_top5") ++= userSL2FeedNewsDurationFcNewsContentTagV2Top5Fill
        featureMap("user_sl2_feed_news_duration_fc_news_content_tag_v2_top5_length") += userSL2FeedNewsDurationFcNewsContentTagV2Top5.length

        val userSL2FeedNewsDurationFcNewsTitleTagV2Top5: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_duration_fc_v2", "news_original_title_tag_v2", userJSONObject, "json", 5).getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSL2FeedNewsDurationFcNewsTitleTagV2Top5Fill: Array[Int] = userSL2FeedNewsDurationFcNewsTitleTagV2Top5 ++ new Array[Int](1000 - userSL2FeedNewsDurationFcNewsTitleTagV2Top5.length)
        featureMap("user_sl2_feed_news_duration_fc_news_title_tag_v2_top5") ++= userSL2FeedNewsDurationFcNewsTitleTagV2Top5Fill
        featureMap("user_sl2_feed_news_duration_fc_news_title_tag_v2_top5_length") += userSL2FeedNewsDurationFcNewsTitleTagV2Top5.length

        val userSL2FeedNewsDurationFcNewsTopicV2Top5: Array[Int] = Tools.parseFCJsonFeature("user_sl_feed_news_duration_fc_v2", "news_topic_vec_v2", userJSONObject, "json", 5).getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSL2FeedNewsDurationFcNewsTopicV2Top5Fill: Array[Int] = userSL2FeedNewsDurationFcNewsTopicV2Top5 ++ new Array[Int](1000 - userSL2FeedNewsDurationFcNewsTopicV2Top5.length)
        featureMap("user_sl2_feed_news_duration_fc_news_topic_v2_top5") ++= userSL2FeedNewsDurationFcNewsTopicV2Top5Fill
        featureMap("user_sl2_feed_news_duration_fc_news_topic_v2_top5_length") += userSL2FeedNewsDurationFcNewsTopicV2Top5.length
        //*****************************json格式新序列相关特征 2020-12-10*****************************

        // 序列相关
        val userSmartLauncherFeedNewsShowFcNewsId = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_id").getT2.map(_.hashCode)
        val userSmartLauncherFeedNewsShowFcNewsIdFill = userSmartLauncherFeedNewsShowFcNewsId ++ new Array[Int](300 - userSmartLauncherFeedNewsShowFcNewsId.length)
        featureMap("user_smart_launcher_feed_news_show_fc_news_id") ++= userSmartLauncherFeedNewsShowFcNewsIdFill
        featureMap("user_smart_launcher_feed_news_show_fc_news_id_length") += userSmartLauncherFeedNewsShowFcNewsId.length
        val userSmartLauncherFeedNewsShowFcNewsSourceLevel1 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_source_level1").getT2.map(_.toInt)
        val userSmartLauncherFeedNewsShowFcNewsSourceLevel1Fill = userSmartLauncherFeedNewsShowFcNewsSourceLevel1 ++ new Array[Int](300 - userSmartLauncherFeedNewsShowFcNewsSourceLevel1.length)
        featureMap("user_smart_launcher_feed_news_show_fc_news_source_level1") ++= userSmartLauncherFeedNewsShowFcNewsSourceLevel1Fill
        featureMap("user_smart_launcher_feed_news_show_fc_news_source_level1_length") += userSmartLauncherFeedNewsShowFcNewsSourceLevel1.length
        val userSmartLauncherFeedNewsShowFcNewsSourceLevel2 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_source_level2").getT2.map(_.toInt)
        val userSmartLauncherFeedNewsShowFcNewsSourceLevel2Fill = userSmartLauncherFeedNewsShowFcNewsSourceLevel2 ++ new Array[Int](300 - userSmartLauncherFeedNewsShowFcNewsSourceLevel2.length)
        featureMap("user_smart_launcher_feed_news_show_fc_news_source_level2") ++= userSmartLauncherFeedNewsShowFcNewsSourceLevel2Fill
        featureMap("user_smart_launcher_feed_news_show_fc_news_source_level2_length") += userSmartLauncherFeedNewsShowFcNewsSourceLevel2.length
        val userSmartLauncherFeedNewsShowFcNewsTopCategoryV3 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_top_category_v3").getT2.map(_.toInt)
        val userSmartLauncherFeedNewsShowFcNewsTopCategoryV3Fill = userSmartLauncherFeedNewsShowFcNewsTopCategoryV3 ++ new Array[Int](300 - userSmartLauncherFeedNewsShowFcNewsTopCategoryV3.length)
        featureMap("user_smart_launcher_feed_news_show_fc_news_top_category_v3") ++= userSmartLauncherFeedNewsShowFcNewsTopCategoryV3Fill
        featureMap("user_smart_launcher_feed_news_show_fc_news_top_category_v3_length") += userSmartLauncherFeedNewsShowFcNewsTopCategoryV3.length
        val userSmartLauncherFeedNewsShowFcNewsSubCategoryV3 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_sub_category_v3").getT2.map(_.toInt)
        val userSmartLauncherFeedNewsShowFcNewsSubCategoryV3Fill = userSmartLauncherFeedNewsShowFcNewsSubCategoryV3 ++ new Array[Int](300 - userSmartLauncherFeedNewsShowFcNewsSubCategoryV3.length)
        featureMap("user_smart_launcher_feed_news_show_fc_news_sub_category_v3") ++= userSmartLauncherFeedNewsShowFcNewsSubCategoryV3Fill
        featureMap("user_smart_launcher_feed_news_show_fc_news_sub_category_v3_length") += userSmartLauncherFeedNewsShowFcNewsSubCategoryV3.length
        val userSmartLauncherFeedNewsShowFcNewsContentTagTop5 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_content_tag_top5").getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSmartLauncherFeedNewsShowFcNewsContentTagTop5Fill = userSmartLauncherFeedNewsShowFcNewsContentTagTop5 ++ new Array[Int](1000 - userSmartLauncherFeedNewsShowFcNewsContentTagTop5.length)
        featureMap("user_smart_launcher_feed_news_show_fc_news_content_tag_top5") ++= userSmartLauncherFeedNewsShowFcNewsContentTagTop5Fill
        featureMap("user_smart_launcher_feed_news_show_fc_news_content_tag_top5_length") += userSmartLauncherFeedNewsShowFcNewsContentTagTop5.length

        val userSmartLauncherFeedNewsShowFcNewsTitleTagTop5 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_title_tag_top5").getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSmartLauncherFeedNewsShowFcNewsTitleTagTop5Fill = userSmartLauncherFeedNewsShowFcNewsTitleTagTop5 ++ new Array[Int](1000 - userSmartLauncherFeedNewsShowFcNewsTitleTagTop5.length)
        featureMap("user_smart_launcher_feed_news_show_fc_news_title_tag_top5") ++= userSmartLauncherFeedNewsShowFcNewsTitleTagTop5Fill
        featureMap("user_smart_launcher_feed_news_show_fc_news_title_tag_top5_length") += userSmartLauncherFeedNewsShowFcNewsTitleTagTop5.length
        val userSmartLauncherFeedNewsShowFcNewsTopicTop5 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_topic_top5").getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSmartLauncherFeedNewsShowFcNewsTopicTop5Fill = userSmartLauncherFeedNewsShowFcNewsTopicTop5 ++ Array.fill[Int](1000 - userSmartLauncherFeedNewsShowFcNewsTopicTop5.length)(2000)
        featureMap("user_smart_launcher_feed_news_show_fc_news_topic_top5") ++= userSmartLauncherFeedNewsShowFcNewsTopicTop5Fill
        featureMap("user_smart_launcher_feed_news_show_fc_news_topic_top5_length") += userSmartLauncherFeedNewsShowFcNewsTopicTop5.length
        val userSmartLauncherFeedNewsClickFcNewsId = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_id").getT2.map(_.hashCode)
        val userSmartLauncherFeedNewsClickFcNewsIdFill = userSmartLauncherFeedNewsClickFcNewsId ++ new Array[Int](300 - userSmartLauncherFeedNewsClickFcNewsId.length)
        featureMap("user_smart_launcher_feed_news_click_fc_news_id") ++= userSmartLauncherFeedNewsClickFcNewsIdFill
        featureMap("user_smart_launcher_feed_news_click_fc_news_id_length") += userSmartLauncherFeedNewsClickFcNewsId.length
        val userSmartLauncherFeedNewsClickFcNewsSourceLevel1 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_source_level1").getT2.map(_.toInt)
        val userSmartLauncherFeedNewsClickFcNewsSourceLevel1Fill = userSmartLauncherFeedNewsClickFcNewsSourceLevel1 ++ new Array[Int](300 - userSmartLauncherFeedNewsClickFcNewsSourceLevel1.length)
        featureMap("user_smart_launcher_feed_news_click_fc_news_source_level1") ++= userSmartLauncherFeedNewsClickFcNewsSourceLevel1Fill
        featureMap("user_smart_launcher_feed_news_click_fc_news_source_level1_length") += userSmartLauncherFeedNewsClickFcNewsSourceLevel1.length
        val userSmartLauncherFeedNewsClickFcNewsSourceLevel2 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_source_level2").getT2.map(_.toInt)
        val userSmartLauncherFeedNewsClickFcNewsSourceLevel2Fill = userSmartLauncherFeedNewsClickFcNewsSourceLevel2 ++ new Array[Int](300 - userSmartLauncherFeedNewsClickFcNewsSourceLevel2.length)
        featureMap("user_smart_launcher_feed_news_click_fc_news_source_level2") ++= userSmartLauncherFeedNewsClickFcNewsSourceLevel2Fill
        featureMap("user_smart_launcher_feed_news_click_fc_news_source_level2_length") += userSmartLauncherFeedNewsClickFcNewsSourceLevel2.length
        val userSmartLauncherFeedNewsClickFcNewsTopCategoryV3 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_top_category_v3").getT2.map(_.toInt)
        val userSmartLauncherFeedNewsClickFcNewsTopCategoryV3Fill = userSmartLauncherFeedNewsClickFcNewsTopCategoryV3 ++ new Array[Int](300 - userSmartLauncherFeedNewsClickFcNewsTopCategoryV3.length)
        featureMap("user_smart_launcher_feed_news_click_fc_news_top_category_v3") ++= userSmartLauncherFeedNewsClickFcNewsTopCategoryV3Fill

        featureMap("user_smart_launcher_feed_news_click_fc_news_top_category_v3_length") += userSmartLauncherFeedNewsClickFcNewsTopCategoryV3.length
        val userSmartLauncherFeedNewsClickFcNewsSubCategoryV3 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_sub_category_v3").getT2.map(_.toInt)
        val userSmartLauncherFeedNewsClickFcNewsSubCategoryV3Fill = userSmartLauncherFeedNewsClickFcNewsSubCategoryV3 ++ new Array[Int](300 - userSmartLauncherFeedNewsClickFcNewsSubCategoryV3.length)
        featureMap("user_smart_launcher_feed_news_click_fc_news_sub_category_v3") ++= userSmartLauncherFeedNewsClickFcNewsSubCategoryV3Fill
        featureMap("user_smart_launcher_feed_news_click_fc_news_sub_category_v3_length") += userSmartLauncherFeedNewsClickFcNewsSubCategoryV3.length
        val userSmartLauncherFeedNewsClickFcNewsContentTagTop5 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_content_tag_top5").getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSmartLauncherFeedNewsClickFcNewsContentTagTop5Fill = userSmartLauncherFeedNewsClickFcNewsContentTagTop5 ++ new Array[Int](1000 - userSmartLauncherFeedNewsClickFcNewsContentTagTop5.length)
        featureMap("user_smart_launcher_feed_news_click_fc_news_content_tag_top5") ++= userSmartLauncherFeedNewsClickFcNewsContentTagTop5Fill
        featureMap("user_smart_launcher_feed_news_click_fc_news_content_tag_top5_length") += userSmartLauncherFeedNewsClickFcNewsContentTagTop5.length
        val userSmartLauncherFeedNewsClickFcNewsTitleTagTop5 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_title_tag_top5").getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSmartLauncherFeedNewsClickFcNewsTitleTagTop5Fill = userSmartLauncherFeedNewsClickFcNewsTitleTagTop5 ++ new Array[Int](1000 - userSmartLauncherFeedNewsClickFcNewsTitleTagTop5.length)
        featureMap("user_smart_launcher_feed_news_click_fc_news_title_tag_top5") ++= userSmartLauncherFeedNewsClickFcNewsTitleTagTop5Fill
        featureMap("user_smart_launcher_feed_news_click_fc_news_title_tag_top5_length") += userSmartLauncherFeedNewsClickFcNewsTitleTagTop5.length
        val userSmartLauncherFeedNewsClickFcNewsTopicTop5 = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_topic_top5").getT2.flatMap(_.split("#")).map(_.toInt).slice(0, 1000)
        val userSmartLauncherFeedNewsClickFcNewsTopicTop5Fill = userSmartLauncherFeedNewsClickFcNewsTopicTop5 ++ Array.fill[Int](1000 - userSmartLauncherFeedNewsClickFcNewsTopicTop5.length)(2000)
        featureMap("user_smart_launcher_feed_news_click_fc_news_topic_top5") ++= userSmartLauncherFeedNewsClickFcNewsTopicTop5Fill
        featureMap("user_smart_launcher_feed_news_click_fc_news_topic_top5_length") += userSmartLauncherFeedNewsClickFcNewsTopicTop5.length
        var userQueryIntentionDenseVec = userJSONObject.getObject("user_query_intention", classOf[Vector])
        try {
          if (userQueryIntentionDenseVec.getIndices().length != userQueryIntentionDenseVec.getValues().length && userQueryIntentionDenseVec.getIndices().length == 0) {
            userQueryIntentionDenseVec.setIndices(null);
          }
        } catch {
          case e => {
            userQueryIntentionDenseVec = Vector.builder(16).index(Array(0)).value(Array(0.0F)).build()
          }
        }
        val (userDislikeTopicIndices, userDislikeTopicValues) = Tools.vec2TFSparseVec(userJSONObject.getObject("user_dislike_topic", classOf[Vector]), defaultTopicVec)
        featureMap("user_dislike_topic_indices") ++= userDislikeTopicIndices
        featureMap("user_dislike_topic_values") ++= userDislikeTopicValues

        //news feature
        val news_id = itemJSONObject.getString("newsId")
        featureMap("news_id") += news_id
        featureMap("news_id_hash") += itemJSONObject.getString("newsId").hashCode
        featureMap("news_id_hash_mod") += itemJSONObject.getString("newsId").hashCode % 1000000
        val newsTopcategoryV3Vec = itemJSONObject.getObject("news_topcategory_v3", classOf[Vector])
        val newsTopcategoryV3VecMaxIndex = if (newsTopcategoryV3Vec.getValues.isEmpty) 0 else newsTopcategoryV3Vec.getValues.indexOf(newsTopcategoryV3Vec.getValues.max)
        val newsTopcategoryV3VecIndices = newsTopcategoryV3Vec.getIndices()
        val newsSubcategoryV3Vec = itemJSONObject.getObject("news_subcategory_v3", classOf[Vector])
        val newsSubcategoryV3VecMaxIndex = if (newsSubcategoryV3Vec.getValues.isEmpty) 0 else newsSubcategoryV3Vec.getValues.indexOf(newsSubcategoryV3Vec.getValues.max)
        val newsSubcategoryV3VecIndices = newsSubcategoryV3Vec.getIndices()
        val newsTopcategoryV3 = if (newsTopcategoryV3VecIndices.isEmpty) 0 else newsTopcategoryV3VecIndices(newsTopcategoryV3VecMaxIndex)
        val newsSubcategoryV3 = if (newsSubcategoryV3VecIndices.isEmpty) 0 else newsSubcategoryV3VecIndices(newsSubcategoryV3VecMaxIndex)

        //新增物料特征 2020-12-18
        val newsTopcategoryV3UpdateVec: Vector = itemJSONObject.getObject("top_category_v3_update", classOf[Vector])
        val newsTopcategoryV3UpdateVecMaxIndex: Int = if (newsTopcategoryV3UpdateVec.getValues.isEmpty) 0 else newsTopcategoryV3UpdateVec.getValues.indexOf(newsTopcategoryV3UpdateVec.getValues.max)
        val newsTopcategoryV3UpdateVecIndices: Array[Int] = newsTopcategoryV3UpdateVec.getIndices()
        val newsTopcategoryV3Update: Int = if (newsTopcategoryV3UpdateVecIndices.isEmpty) 0 else newsTopcategoryV3UpdateVecIndices(newsTopcategoryV3UpdateVecMaxIndex)
        featureMap("news_topcategory_v3_update") += newsTopcategoryV3Update
        val (newsTopcategoryV3UpdateIndices, newsTopcategoryV3UpdateValues) = vec2TFSparseVec(itemJSONObject.getObject("top_category_v3_update", classOf[Vector]))
        featureMap("news_topcategory_v3_update_indices") ++= newsTopcategoryV3UpdateIndices
        featureMap("news_topcategory_v3_update_values") ++= newsTopcategoryV3UpdateValues

        val newsSubcategoryV3UpdateVec: Vector = itemJSONObject.getObject("sub_category_v3_update", classOf[Vector])
        val newsSubcategoryV3UpdateVecMaxIndex: Int = if (newsSubcategoryV3UpdateVec.getValues.isEmpty) 0 else newsSubcategoryV3UpdateVec.getValues.indexOf(newsSubcategoryV3UpdateVec.getValues.max)
        val newsSubcategoryV3UpdateVecIndices: Array[Int] = newsSubcategoryV3UpdateVec.getIndices()
        val newsSubcategoryV3Update: Int = if (newsSubcategoryV3UpdateVecIndices.isEmpty) 0 else newsSubcategoryV3UpdateVecIndices(newsSubcategoryV3UpdateVecMaxIndex)
        featureMap("news_subcategory_v3_update") += newsSubcategoryV3Update
        val (newsSubcategoryV3UpdateIndices, newsSubcategoryV3UpdateValues) = vec2TFSparseVec(itemJSONObject.getObject("sub_category_v3_update", classOf[Vector]))
        featureMap("news_subcategory_v3_update_indices") ++= newsSubcategoryV3UpdateIndices
        featureMap("news_subcategory_v3_update_values") ++= newsSubcategoryV3UpdateValues

        val newsThirdcategoryV3UpdateVec: Vector = itemJSONObject.getObject("third_category_v3_update", classOf[Vector])
        val newsThirdcategoryV3UpdateVecMaxIndex: Int = if (newsThirdcategoryV3UpdateVec.getValues.isEmpty) 0 else newsThirdcategoryV3UpdateVec.getValues.indexOf(newsThirdcategoryV3UpdateVec.getValues.max)
        val newsThirdcategoryV3UpdateVecIndices: Array[Int] = newsThirdcategoryV3UpdateVec.getIndices()
        val newsThirdcategoryV3Update: Int = if (newsThirdcategoryV3UpdateVecIndices.isEmpty) 0 else newsThirdcategoryV3UpdateVecIndices(newsThirdcategoryV3UpdateVecMaxIndex)
        featureMap("news_thirdcategory_v3_update") += newsThirdcategoryV3Update
        val (newsThirdcategoryV3UpdateIndices, newsThirdcategoryV3UpdateValues) = vec2TFSparseVec(itemJSONObject.getObject("third_category_v3_update", classOf[Vector]))
        featureMap("news_thirdcategory_v3_update_indices") ++= newsThirdcategoryV3UpdateIndices
        featureMap("news_thirdcategory_v3_update_values") ++= newsThirdcategoryV3UpdateValues


        val newsOriginalTitleTagV2Map: Map[String, Double] = if (Tools.jsonGetStringMap(itemJSONObject, "original_title_tag_vec_v2").isEmpty) Map("" -> 0.0) else Tools.jsonGetStringMap(itemJSONObject, "original_title_tag_vec_v2")
        val newsOriginalTitleTagV2Arr: Array[(Int, Double)] = newsOriginalTitleTagV2Map.toArray.map(value => (Math.abs(value._1.hashCode % 1000000), value._2)).groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val newsOriginalTitleTagV2Vec: Vector = Vector.builder(1000000).index(newsOriginalTitleTagV2Arr.map(_._1)).value(newsOriginalTitleTagV2Arr.map(_._2.toFloat)).build()
        val (newsOriginalTitleTagV2VecIndices, newsOriginalTitleTagV2VecValues) = vec2TFSparseVec(newsOriginalTitleTagV2Vec)
        featureMap("news_original_title_tag_vec_v2_indices") ++= newsOriginalTitleTagV2VecIndices
        featureMap("news_original_title_tag_vec_v2_values") ++= newsOriginalTitleTagV2VecValues

        val newsOriginalContentTagV2Map: Map[String, Double] = if (Tools.jsonGetStringMap(itemJSONObject, "original_content_tag_vec_v2").isEmpty) Map("" -> 0.0) else Tools.jsonGetStringMap(itemJSONObject, "original_content_tag_vec_v2")
        val newsOriginalContentTagV2Arr: Array[(Int, Double)] = newsOriginalContentTagV2Map.toArray.map(value => (Math.abs(value._1.hashCode % 1000000), value._2)).groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val newsOriginalContentTagV2Vec: Vector = Vector.builder(1000000).index(newsOriginalContentTagV2Arr.map(_._1)).value(newsOriginalContentTagV2Arr.map(_._2.toFloat)).build()
        val (newsOriginalContentTagV2VecIndices, newsOriginalContentTagV2VecValues) = vec2TFSparseVec(newsOriginalContentTagV2Vec)
        featureMap("news_original_content_tag_vec_v2_indices") ++= newsOriginalContentTagV2VecIndices
        featureMap("news_original_content_tag_vec_v2_values") ++= newsOriginalContentTagV2VecValues

        val newsTopicVecV2: Vector = itemJSONObject.getObject("topic_vec_v2", classOf[Vector])
        val (newsTopicVecV2Indices, newsTopicVecV2Values) = vec2TFSparseVec(newsTopicVecV2, defaultTopicVec)
        featureMap("news_topic_vec_v2_indices") ++= newsTopicVecV2Indices
        featureMap("news_topic_vec_v2_values") ++= newsTopicVecV2Values
        //新增物料特征 2020-12-18

        featureMap("news_topcategory_v3") += newsTopcategoryV3
        featureMap("news_subcategory_v3") += newsSubcategoryV3
        val (newsTopcategoryV3Indices, newsTopcategoryV3Values) = vec2TFSparseVec(itemJSONObject.getObject("news_topcategory_v3", classOf[Vector]))
        featureMap("news_topcategory_v3_indices") ++= newsTopcategoryV3Indices
        featureMap("news_topcategory_v3_values") ++= newsTopcategoryV3Values
        val (newsSubcategoryV3Indices, newsSubcategoryV3Values) = vec2TFSparseVec(itemJSONObject.getObject("news_subcategory_v3", classOf[Vector]))
        featureMap("news_subcategory_v3_indices") ++= newsSubcategoryV3Indices
        featureMap("news_subcategory_v3_values") ++= newsSubcategoryV3Values
        val news_topic_vec = itemJSONObject.getObject("news_topic_vec", classOf[Vector])
        val (newsTopicVecIndices, newsTopicVecValues) = Tools.vec2TFSparseVec(news_topic_vec, defaultTopicVec)

        featureMap("news_topic_vec_indices") ++= newsTopicVecIndices
        featureMap("news_topic_vec_values") ++= newsTopicVecValues
        val newsBertVec = itemJSONObject.getObject("news_bert_vec", classOf[Vector]).toDense.getValues.map(_.toDouble)
        featureMap("news_bert_vec") ++= newsBertVec
        featureMap("news_original_category_encode") += itemJSONObject.getIntValue("news_original_category_encode")
        val news_source_level1 = itemJSONObject.getIntValue("news_source_level1")
        featureMap("news_source_level1") += news_source_level1
        val news_source_level2 = itemJSONObject.getIntValue("news_source_level2")
        featureMap("news_source_level2") += news_source_level2
        featureMap("news_region") += itemJSONObject.getIntValue("news_region")
        featureMap("news_is_capital") += itemJSONObject.getIntValue("news_is_capital")
        featureMap("news_city_level") += itemJSONObject.getIntValue("news_city_level")
        featureMap("news_province") += itemJSONObject.getIntValue("news_province")
        featureMap("news_city") += itemJSONObject.getIntValue("news_city")
        featureMap("news_content_length") += itemJSONObject.getIntValue("news_content_length")
        featureMap("news_title_length") += itemJSONObject.getIntValue("news_title_length")
        featureMap("news_img_type") += itemJSONObject.getIntValue("news_img_type")
        featureMap("news_article_type") += itemJSONObject.getIntValue("news_article_type")
        featureMap("news_img_count") += itemJSONObject.getIntValue("news_img_count")
        val newsPositionString = itemJSONObject.getString("news_position")
        val newsPosition = if (newsPositionString == null || newsPositionString == "") 0 else newsPositionString.toInt
        featureMap("news_position") += newsPosition
        //新闻展示时间
        featureMap("news_show_time") += eventTime.toString
        featureMap("news_show_hour") += SceneEncoder.getHourEncode(eventTime)
        featureMap("news_show_week_of_day") += SceneEncoder.getWeekOfDayEncode(eventTime)
        featureMap("news_show_is_holiday") += SceneEncoder.isHolidayEncode(eventTime)
        val newsPublishTimeitem = itemJSONObject.getLongValue("news_publish_time")
        val newsShowDuration = (eventTime - newsPublishTimeitem) / (60 * 60 * 1000)
        val newsShowHourDuration = if (newsShowDuration <= 0 || newsShowDuration >= 15 * 24) 0 else newsShowDuration
        featureMap("news_show_hour_duration") += newsShowHourDuration.toDouble

        val videoDuration = itemJSONObject.getIntValue("video_duration").toDouble
        val videoDurationNormalize = if (videoDuration < 0 || videoDuration == null) 0.0D else videoDuration.toDouble
        featureMap("video_duration") += videoDurationNormalize
        // 阅读时长
        val readDuration = itemJSONObject.getIntValue("duration").toDouble
        val readDurationNormalize = if (readDuration <= 0) 0.0D else readDuration.toDouble / 1000
        featureMap("read_duration") += readDurationNormalize
        val pcr = if (videoDuration > 0.0) readDurationNormalize / videoDuration else 0.0
        val pcrFix = math.min(1.0, pcr)
        featureMap("pcr") += pcrFix
        val news_source = itemJSONObject.getString("news_source")
        featureMap("news_source") += news_source
        featureMap("news_author_hash") += itemJSONObject.getIntValue("news_author_hash")
        featureMap("news_elite") += itemJSONObject.getIntValue("news_elite")

        //cosfeature
        featureMap("cos_topic_news_user_long") += itemJSONObject.getDoubleValue("cos_topic_news_user_long")
        featureMap("cos_topic_news_user_short") += itemJSONObject.getDoubleValue("cos_topic_news_user_short")

        //realTime feature
        val (userNewsRtShowTopicIndices, userNewsRtShowTopicValues) = Tools.vec2TFSparseVec(userJSONObject.getObject("user_news_rt_show_topic", classOf[Vector]), defaultTopicVec)
        featureMap("user_news_rt_show_topic_indices") ++= userNewsRtShowTopicIndices
        featureMap("user_news_rt_show_topic_values") ++= userNewsRtShowTopicValues

        //统计CTR
        featureMap("ctr_feeds_news_news_id") += jsonGetOrElseDouble(itemJSONObject, "ctr_feeds_news_news_id")
        featureMap("ctr_feeds_news_news_article_type") += jsonGetOrElseDouble(itemJSONObject, "ctr_feeds_news_news_article_type")
        featureMap("ctr_feeds_news_news_city") += jsonGetOrElseDouble(itemJSONObject, "ctr_feeds_news_news_city")
        featureMap("ctr_feeds_news_news_sub_category_v3") += jsonGetOrElseDouble(itemJSONObject, "ctr_feeds_news_news_sub_category_v3")


        featureMap("ctr_feeds_news_news_top_category_v3") += jsonGetOrElseDouble(itemJSONObject, "ctr_feeds_news_news_top_category_v3")
        featureMap("ctr_feeds_news_news_source_level1") += jsonGetOrElseDouble(itemJSONObject, "ctr_feeds_news_news_source_level1")
        featureMap("ctr_feeds_news_news_source_level2") += jsonGetOrElseDouble(itemJSONObject, "ctr_feeds_news_news_source_level2")
        featureMap("ctr_feeds_news_news_author_nomod") += jsonGetOrElseDouble(itemJSONObject, "ctr_feeds_news_news_author_nomod")

        // Tag hash不取余
        // 注意这里因为不做prebatch所以tag可以不取余。否则不知道最大长度500000没办法reshape回来。Vector indices大于length没有报错。
        val newsTagVecMap = if (Tools.jsonGetMap(itemJSONObject, "news_tag_vec").isEmpty) Map(0 -> 0.0) else Tools.jsonGetMap(itemJSONObject, "news_tag_vec")
        val newsTagVecHashArr = newsTagVecMap.toArray.map { x => (x._1.hashCode, x._2) }.groupBy(_._1).mapValues(_.map(_._2).sum).toArray.sortBy(_._1)
        val newsTagVec = Vector.builder(500000).index(newsTagVecHashArr.map(_._1)).value(newsTagVecHashArr.map(_._2.toFloat)).build()
        val (newsTagVecIndices, newsTagVecValues) = vec2TFSparseVec(newsTagVec)
        featureMap("news_tag_vec_indices") ++= newsTagVecIndices
        featureMap("news_tag_vec_values") ++= newsTagVecValues
        val userNewsRtShowTagVecMap = if (Tools.jsonGetMap(userJSONObject, "user_news_rt_show_tag").isEmpty) Map(0 -> 0.0) else Tools.jsonGetMap(userJSONObject, "user_news_rt_show_tag")
        val userNewsRtShowTagVecHashArr = userNewsRtShowTagVecMap.toArray.map { x => (x._1.hashCode, x._2) }.groupBy(_._1).mapValues(_.map(_._2).sum).toArray.sortBy(_._1)
        val userNewsRtShowTagVec = Vector.builder(500000).index(userNewsRtShowTagVecHashArr.map(_._1)).value(userNewsRtShowTagVecHashArr.map(_._2.toFloat)).build()
        val (userNewsRtShowTagVecIndices, userNewsRtShowTagVecValues) = vec2TFSparseVec(userNewsRtShowTagVec)
        featureMap("user_news_rt_show_tag_vec_indices") ++= userNewsRtShowTagVecIndices
        featureMap("user_news_rt_show_tag_vec_values") ++= userNewsRtShowTagVecValues
        val userSmartLauncherFeedNewsShowFcNewsIdString = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_id").getT2
        val userSmartLauncherFeedNewsShowFcNewsIdStringFillArr = new Array[String](300 - userSmartLauncherFeedNewsShowFcNewsIdString.length)
        val userSmartLauncherFeedNewsShowFcNewsIdStringFill = userSmartLauncherFeedNewsShowFcNewsIdString ++ userSmartLauncherFeedNewsShowFcNewsIdStringFillArr.map { s => "" }
        featureMap("user_smart_launcher_feed_news_show_fc_news_id_string") ++= userSmartLauncherFeedNewsShowFcNewsIdStringFill
        featureMap("user_smart_launcher_feed_news_show_fc_news_id_string_length") += userSmartLauncherFeedNewsShowFcNewsIdString.length
        val userSmartLauncherFeedNewsClickFcNewsIdString = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_id").getT2
        val userSmartLauncherFeedNewsClickFcNewsIdStringFillArr = new Array[String](300 - userSmartLauncherFeedNewsClickFcNewsIdString.length)
        val userSmartLauncherFeedNewsClickFcNewsIdStringFill = userSmartLauncherFeedNewsClickFcNewsIdString ++ userSmartLauncherFeedNewsClickFcNewsIdStringFillArr.map { s => "" }
        featureMap("user_smart_launcher_feed_news_click_fc_news_id_string") ++= userSmartLauncherFeedNewsClickFcNewsIdStringFill
        featureMap("user_smart_launcher_feed_news_click_fc_news_id_string_length") += userSmartLauncherFeedNewsClickFcNewsIdString.length


        featureMap("pctr") += itemJSONObject.getDoubleValue("pctr")
        //1.5版本新增特征
        //用户侧
        val userQueryLongTagVecMap = if (Tools.jsonGetMap(userJSONObject, "user_query_long_tag").isEmpty) Map(0 -> 0.0) else Tools.jsonGetMap(userJSONObject, "user_query_long_tag")
        val userQueryLongTagVecArr = userQueryLongTagVecMap.toArray.groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val userQueryLongTagVec = Vector.builder(500000).index(userQueryLongTagVecArr.map(_._1)).value(userQueryLongTagVecArr.map(_._2.toFloat)).build()
        val (userQueryLongTagVecIndices, userQueryLongTagVecValues) = vec2TFSparseVec(userQueryLongTagVec)
        featureMap("user_query_long_tag_indices") ++= userQueryLongTagVecIndices
        featureMap("user_query_long_tag_values") ++= userQueryLongTagVecValues
        val userNewsLongTopCategoryV3 = userJSONObject.getObject("user_news_long_top_category_v3", classOf[Vector])
        val (userNewsLongTopCategoryV3Indices, userNewsLongTopCategoryV3Values) = vec2TFSparseVec(userNewsLongTopCategoryV3)
        featureMap("user_news_long_top_category_v3_indices") ++= userNewsLongTopCategoryV3Indices
        featureMap("user_news_long_top_category_v3_values") ++= userNewsLongTopCategoryV3Values
        val (userNewsShortTopCategoryV3Indices, userNewsShortTopCategoryV3Values) = vec2TFSparseVec(userJSONObject.getObject("user_news_short_top_category_v3", classOf[Vector]))
        featureMap("user_news_short_top_category_v3_indices") ++= userNewsShortTopCategoryV3Indices
        featureMap("user_news_short_top_category_v3_values") ++= userNewsShortTopCategoryV3Values
        val user_news_long_sub_category_v3 = userJSONObject.getObject("user_news_long_sub_category_v3", classOf[Vector])
        val (userNewsLongSubCategoryV3Indices, userNewsLongSubCategoryV3Values) = vec2TFSparseVec(user_news_long_sub_category_v3)
        featureMap("user_news_long_sub_category_v3_indices") ++= userNewsLongSubCategoryV3Indices
        featureMap("user_news_long_sub_category_v3_values") ++= userNewsLongSubCategoryV3Values
        val (userNewsShortSubCategoryV3Indices, userNewsShortSubCategoryV3Values) = vec2TFSparseVec(userJSONObject.getObject("user_news_short_sub_category_v3", classOf[Vector]))
        featureMap("user_news_short_sub_category_v3_indices") ++= userNewsShortSubCategoryV3Indices
        featureMap("user_news_short_sub_category_v3_values") ++= userNewsShortSubCategoryV3Values
        val userNewsTitleTagPreferenceMap = if (Tools.jsonGetMap(userJSONObject, "user_news_title_tag_preference").isEmpty) Map(0 -> 0.0) else Tools.jsonGetMap(userJSONObject, "user_news_title_tag_preference")
        val userNewsTitleTagPreferenceArr = userNewsTitleTagPreferenceMap.toArray.groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val userNewsTitleTagPreferenceVec = Vector.builder(500000).index(userNewsTitleTagPreferenceArr.map(_._1)).value(userNewsTitleTagPreferenceArr.map(_._2.toFloat)).build()
        val (userNewsTitleTagPreferenceIndices, userNewsTitleTagPreferenceValues) = vec2TFSparseVec(userNewsTitleTagPreferenceVec)
        featureMap("user_news_title_tag_preference_indices") ++= userNewsTitleTagPreferenceIndices
        featureMap("user_news_title_tag_preference_values") ++= userNewsTitleTagPreferenceValues
        //jovi特征


        val newsTitleTagVecMap = if (Tools.jsonGetMap(itemJSONObject, "news_title_tag_vec").isEmpty) Map(0 -> 0.0) else Tools.jsonGetMap(itemJSONObject, "news_title_tag_vec")
        val newsTitleTagVecArr = newsTitleTagVecMap.toArray.groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val newsTitleTagVec = Vector.builder(500000).index(newsTitleTagVecArr.map(_._1)).value(newsTitleTagVecArr.map(_._2.toFloat)).build()
        val (newsTitleTagVecIndices, newsTitleTagVecValues) = vec2TFSparseVec(newsTitleTagVec)
        featureMap("news_title_tag_vec_indices") ++= newsTitleTagVecIndices
        featureMap("news_title_tag_vec_values") ++= newsTitleTagVecValues

        val newsContentTagVecMap = if (Tools.jsonGetMap(itemJSONObject, "news_content_tag_vec").isEmpty) Map(0 -> 0.0) else Tools.jsonGetMap(itemJSONObject, "news_content_tag_vec")
        val newsContentTagVecArr = newsContentTagVecMap.toArray.groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val newsContentTagVec = Vector.builder(500000).index(newsContentTagVecArr.map(_._1)).value(newsContentTagVecArr.map(_._2.toFloat)).build()
        val (newsContentTagVecIndices, newsContentTagVecValues) = vec2TFSparseVec(newsContentTagVec)
        featureMap("news_content_tag_vec_indices") ++= newsContentTagVecIndices
        featureMap("news_content_tag_vec_values") ++= newsContentTagVecValues

        val newsCoreTagVecMap = if (Tools.jsonGetMap(itemJSONObject, "news_core_tag_vec").isEmpty) Map(0 -> 0.0) else Tools.jsonGetMap(itemJSONObject, "news_core_tag_vec")
        val newsCoreTagVecArr = newsCoreTagVecMap.toArray.groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val newsCoreTagVec = Vector.builder(500000).index(newsCoreTagVecArr.map(_._1)).value(newsCoreTagVecArr.map(_._2.toFloat)).build()
        val (newsCoreTagVecIndices, newsCoreTagVecValues) = vec2TFSparseVec(newsCoreTagVec)
        featureMap("news_core_tag_vec_indices") ++= newsCoreTagVecIndices
        featureMap("news_core_tag_vec_values") ++= newsCoreTagVecValues

        featureMap("news_quality_level") += itemJSONObject.getIntValue("news_quality_level")
        featureMap("news_sex_profile") ++= itemJSONObject.getObject("news_sex_profile", classOf[Vector]).toDense.getValues.map(_.toDouble)
        featureMap("news_age_profile") ++= itemJSONObject.getObject("news_age_profile", classOf[Vector]).toDense.getValues.map(_.toDouble)

        //json格式新序列特征开发  2020-12-15
        val userS2LFeedNewsShowFcNewsIdSequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_id", userJSONObject, "string")
        val userSL2FeedNewsClickFcNewsIdSequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_id", userJSONObject, "string")
        FrequencyControlFeature.addFrequencyControlFeature("id_nv", reqTimestamp, news_id, userS2LFeedNewsShowFcNewsIdSequence, userSL2FeedNewsClickFcNewsIdSequence, featureMap)


        val userS2LFeedNewsDurationFcNewsIdSequence: tuple.Tuple3[Array[Long], Array[String], Array[Double]] = Tools.parseFCJsonDurationFeature("user_sl_feed_news_duration_fc_v2", "news_id", userJSONObject, "string")
        FrequencyControlFeature.addDurationFrequencyControlFeature("id_nv", reqTimestamp, news_id, userS2LFeedNewsDurationFcNewsIdSequence, featureMap)

        val userSL2FeedNewsShowFcNewsTopCategoryV3UpdateSequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "top_category_v3_update", userJSONObject, "json")
        val userSL2FeedNewsClickFcNewsTopCategoryV3UpdateSequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "top_category_v3_update", userJSONObject, "json")
        FrequencyControlFeature.addFrequencyControlFeature("top_category_v3_update_nv", reqTimestamp, newsTopcategoryV3Update.toString, userSL2FeedNewsShowFcNewsTopCategoryV3UpdateSequence, userSL2FeedNewsClickFcNewsTopCategoryV3UpdateSequence, featureMap)

        val userS2LFeedNewsDurationFcNewsTopCategoryV3UpdateSequence: tuple.Tuple3[Array[Long], Array[String], Array[Double]] = Tools.parseFCJsonDurationFeature("user_sl_feed_news_duration_fc_v2", "top_category_v3_update", userJSONObject, "json")
        FrequencyControlFeature.addDurationFrequencyControlFeature("top_category_v3_update_nv", reqTimestamp, newsTopcategoryV3Update.toString, userS2LFeedNewsDurationFcNewsTopCategoryV3UpdateSequence, featureMap)

        val userSL2FeedNewsShowFcNewsSubCategoryV3UpdateSequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "sub_category_v3_update", userJSONObject, "json")
        val userSL2FeedNewsClickFcNewsSubCategoryV3UpdateSequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "sub_category_v3_update", userJSONObject, "json")
        FrequencyControlFeature.addFrequencyControlFeature("sub_category_v3_update_nv", reqTimestamp, newsSubcategoryV3Update.toString, userSL2FeedNewsShowFcNewsSubCategoryV3UpdateSequence, userSL2FeedNewsClickFcNewsSubCategoryV3UpdateSequence, featureMap)

        val userS2LFeedNewsDurationFcNewsSubCategoryV3UpdateSequence: tuple.Tuple3[Array[Long], Array[String], Array[Double]] = Tools.parseFCJsonDurationFeature("user_sl_feed_news_duration_fc_v2", "sub_category_v3_update", userJSONObject, "json")
        FrequencyControlFeature.addDurationFrequencyControlFeature("sub_category_v3_update_nv", reqTimestamp, newsSubcategoryV3Update.toString, userS2LFeedNewsDurationFcNewsSubCategoryV3UpdateSequence, featureMap)


        val userSL2FeedNewsShowFcNewsThirdCategoryV3UpdateSequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "third_category_v3_update", userJSONObject, "json")
        val userSL2FeedNewsClickFcNewsThirdCategoryV3UpdateSequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "third_category_v3_update", userJSONObject, "json")
        FrequencyControlFeature.addFrequencyControlFeature("third_category_v3_update_nv", reqTimestamp, newsThirdcategoryV3Update.toString, userSL2FeedNewsShowFcNewsThirdCategoryV3UpdateSequence, userSL2FeedNewsClickFcNewsThirdCategoryV3UpdateSequence, featureMap)


        val userS2LFeedNewsDurationFcNewsThirdCategoryV3UpdateSequence: tuple.Tuple3[Array[Long], Array[String], Array[Double]] = Tools.parseFCJsonDurationFeature("user_sl_feed_news_duration_fc_v2", "third_category_v3_update", userJSONObject, "json")
        FrequencyControlFeature.addDurationFrequencyControlFeature("third_category_v3_update_nv", reqTimestamp, newsThirdcategoryV3Update.toString, userS2LFeedNewsDurationFcNewsThirdCategoryV3UpdateSequence, featureMap)

        val userSL2FeedNewsShowFcNewsTopCategoryV3Sequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_category_v3", userJSONObject, "json")
        val userSL2FeedNewsClickFcNewsTopCategoryV3Sequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_category_v3", userJSONObject, "json")
        FrequencyControlFeature.addFrequencyControlFeature("top_category_v3_nv", reqTimestamp, newsTopcategoryV3.toString, userSL2FeedNewsShowFcNewsTopCategoryV3Sequence, userSL2FeedNewsClickFcNewsTopCategoryV3Sequence, featureMap)

        val userS2LFeedNewsDurationFcNewsTopCategoryV3Sequence: tuple.Tuple3[Array[Long], Array[String], Array[Double]] = Tools.parseFCJsonDurationFeature("user_sl_feed_news_duration_fc_v2", "news_category_v3", userJSONObject, "json")
        FrequencyControlFeature.addDurationFrequencyControlFeature("top_category_v3_nv", reqTimestamp, newsTopcategoryV3.toString, userS2LFeedNewsDurationFcNewsTopCategoryV3Sequence, featureMap)


        val userSL2FeedNewsShowFcNewsSubCategoryV3Sequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_subcategory_v3", userJSONObject, "json")
        val userSL2FeedNewsClickFcNewsSubCategoryV3Sequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_subcategory_v3", userJSONObject, "json")
        FrequencyControlFeature.addFrequencyControlFeature("sub_category_v3_nv", reqTimestamp, newsSubcategoryV3.toString, userSL2FeedNewsShowFcNewsSubCategoryV3Sequence, userSL2FeedNewsClickFcNewsSubCategoryV3Sequence, featureMap)

        val userS2LFeedNewsDurationFcNewsSubCategoryV3Sequence: tuple.Tuple3[Array[Long], Array[String], Array[Double]] = Tools.parseFCJsonDurationFeature("user_sl_feed_news_duration_fc_v2", "news_subcategory_v3", userJSONObject, "json")
        FrequencyControlFeature.addDurationFrequencyControlFeature("sub_category_v3_nv", reqTimestamp, newsSubcategoryV3.toString, userS2LFeedNewsDurationFcNewsSubCategoryV3Sequence, featureMap)

        val userSL2FeedNewsShowFcNewsContentTagV2Top5Sequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_content_tag_vec_v2", userJSONObject, "json", 5)
        val userSL2FeedNewsClickFcNewsContentTagV2Top5Sequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_content_tag_vec_v2", userJSONObject, "json", 5)
        FrequencyControlFeature.addMutiFrequencyControlFeature("content_tag_v2_top5_nv", reqTimestamp, newsOriginalContentTagV2VecIndices.map(_.toString).toList, 1000000, userSL2FeedNewsShowFcNewsContentTagV2Top5Sequence, userSL2FeedNewsClickFcNewsContentTagV2Top5Sequence, featureMap)


        val userS2LFeedNewsDurationFcNewsContentTagV2Top5Sequence: tuple.Tuple3[Array[Long], Array[String], Array[Double]] = Tools.parseFCJsonDurationFeature("user_sl_feed_news_duration_fc_v2", "news_content_tag_vec_v2", userJSONObject, "json", 5)
        FrequencyControlFeature.addMutiDurationFrequencyControlFeature("content_tag_v2_top5_nv", reqTimestamp, newsOriginalContentTagV2VecIndices.map(_.toString).toList, 1000000, userS2LFeedNewsDurationFcNewsContentTagV2Top5Sequence, featureMap)

        val userSL2FeedNewsShowFcNewsTitleTagV2Top5Sequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_original_title_tag_v2", userJSONObject, "json", 5)
        val userSL2FeedNewsClickFcNewsTitleTagV2Top5Sequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_original_title_tag_v2", userJSONObject, "json", 5)
        FrequencyControlFeature.addMutiFrequencyControlFeature("title_tag_v2_top5_nv", reqTimestamp, newsOriginalTitleTagV2VecIndices.map(_.toString).toList, 1000000, userSL2FeedNewsShowFcNewsTitleTagV2Top5Sequence, userSL2FeedNewsClickFcNewsTitleTagV2Top5Sequence, featureMap)

        val userS2LFeedNewsDurationFcNewsTitleTagV2Top5Sequence: tuple.Tuple3[Array[Long], Array[String], Array[Double]] = Tools.parseFCJsonDurationFeature("user_sl_feed_news_duration_fc_v2", "news_original_title_tag_v2", userJSONObject, "json", 5)
        FrequencyControlFeature.addMutiDurationFrequencyControlFeature("title_tag_v2_top5_nv", reqTimestamp, newsOriginalTitleTagV2VecIndices.map(_.toString).toList, 1000000, userS2LFeedNewsDurationFcNewsTitleTagV2Top5Sequence, featureMap)

        val userSL2FeedNewsShowFcNewsTopicV2Top5Sequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_show_fc_v2", "news_topic_vec_v2", userJSONObject, "json", 5)
        val userSL2FeedNewsClickFcNewsTopicV2Top5Sequence: tuple.Tuple2[Array[Long], Array[String]] = Tools.parseFCJsonFeature("user_sl_feed_news_click_fc_v2", "news_topic_vec_v2", userJSONObject, "json", 5)
        FrequencyControlFeature.addMutiFrequencyControlFeature("topic_v2_top5_nv", reqTimestamp, newsTopicVecV2Indices.map(_.toString).toList, 2001, userSL2FeedNewsShowFcNewsTopicV2Top5Sequence, userSL2FeedNewsClickFcNewsTopicV2Top5Sequence, featureMap)

        val userS2LFeedNewsDurationFcNewsTopicV2Top5Sequence: tuple.Tuple3[Array[Long], Array[String], Array[Double]] = Tools.parseFCJsonDurationFeature("user_sl_feed_news_duration_fc_v2", "news_topic_vec_v2", userJSONObject, "json", 5)
        FrequencyControlFeature.addMutiDurationFrequencyControlFeature("topic_v2_top5_nv", reqTimestamp, newsTopicVecV2Indices.map(_.toString).toList, 2001, userS2LFeedNewsDurationFcNewsTopicV2Top5Sequence, featureMap)

        //json格式新序列特征开发  2020-12-15

        //新频控特征
        val userSmartLauncherFeedNewsShowFcNewsTopCategoryV3Sequence = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_top_category_v3")
        val userSmartLauncherFeedNewsClickFcNewsTopCategoryV3Sequence = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_top_category_v3")


        FrequencyControlFeature.addFrequencyControlFeature("top_category_v3", reqTimestamp, newsTopcategoryV3.toString, userSmartLauncherFeedNewsShowFcNewsTopCategoryV3Sequence, userSmartLauncherFeedNewsClickFcNewsTopCategoryV3Sequence, featureMap)
        val userSmartLauncherFeedNewsShowFcNewsSubCategoryV3Sequence = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_sub_category_v3")
        val userSmartLauncherFeedNewsClickFcNewsSubCategoryV3Sequence = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_sub_category_v3")
        FrequencyControlFeature.addFrequencyControlFeature("sub_category_v3", reqTimestamp, newsSubcategoryV3.toString, userSmartLauncherFeedNewsShowFcNewsSubCategoryV3Sequence, userSmartLauncherFeedNewsClickFcNewsSubCategoryV3Sequence, featureMap)
        val userSmartLauncherFeedNewsShowFcNewsContentTagTop5Sequence = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_content_tag_top5")
        val userSmartLauncherFeedNewsClickFcNewsContentTagTop5Sequence = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_content_tag_top5")
        FrequencyControlFeature.addMutiFrequencyControlFeature("content_tag_top5", reqTimestamp, newsContentTagVecIndices.map(_.toString).toList, 500000, userSmartLauncherFeedNewsShowFcNewsContentTagTop5Sequence, userSmartLauncherFeedNewsClickFcNewsContentTagTop5Sequence, featureMap)
        val userSmartLauncherFeedNewsShowFcNewsTitleTagTop5Sequence = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_title_tag_top5")
        val userSmartLauncherFeedNewsClickFcNewsTitleTagTop5Sequence = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_title_tag_top5")
        FrequencyControlFeature.addMutiFrequencyControlFeature("title_tag_top5", reqTimestamp, newsTitleTagVecIndices.map(_.toString).toList, 500000, userSmartLauncherFeedNewsShowFcNewsTitleTagTop5Sequence, userSmartLauncherFeedNewsClickFcNewsTitleTagTop5Sequence, featureMap)
        val userSmartLauncherFeedNewsShowFcNewsTopicTop5Sequence = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_topic_top5")
        val userSmartLauncherFeedNewsClickFcNewsTopicTop5Sequence = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_topic_top5")
        FrequencyControlFeature.addMutiFrequencyControlFeature("topic_top5", reqTimestamp, newsTopicVecIndices.map(_.toString).toList, 2001, userSmartLauncherFeedNewsShowFcNewsTopicTop5Sequence, userSmartLauncherFeedNewsClickFcNewsTopicTop5Sequence, featureMap)

        /*-----1.6-----*****************************************************************************************************************/
        val user_app_ftype_times = userJSONObject.getObject("user_app_ftype_times", classOf[Vector])
        val (user_app_ftype_timesIndices, user_app_ftype_timesValues) = vec2TFSparseVec(user_app_ftype_times)
        featureMap("user_app_ftype_times_indices") ++= user_app_ftype_timesIndices
        featureMap("user_app_ftype_times_values") ++= user_app_ftype_timesValues


        val user_app_stype_times = userJSONObject.getObject("user_app_stype_times", classOf[Vector])
        val (user_app_stype_timesIndices, user_app_stype_timesValues) = vec2TFSparseVec(user_app_stype_times)
        featureMap("user_app_stype_times_indices") ++= user_app_stype_timesIndices
        featureMap("user_app_stype_times_values") ++= user_app_stype_timesValues
        featureMap("user_app_sum_times") += userJSONObject.getDouble("user_app_sum_times")

        val user_app_ftype_duration = userJSONObject.getObject("user_app_ftype_duration", classOf[Vector])
        val (user_app_ftype_durationIndices, user_app_ftype_durationValues) = vec2TFSparseVec(user_app_ftype_duration)
        featureMap("user_app_ftype_duration_indices") ++= user_app_ftype_durationIndices
        featureMap("user_app_ftype_duration_values") ++= user_app_ftype_durationValues
        val user_app_stype_duration = userJSONObject.getObject("user_app_stype_duration", classOf[Vector])
        val (user_app_stype_durationIndices, user_app_stype_durationValues) = vec2TFSparseVec(user_app_stype_duration)
        featureMap("user_app_stype_duration_indices") ++= user_app_stype_durationIndices
        featureMap("user_app_stype_duration_values") ++= user_app_stype_durationValues
        featureMap("user_app_sum_duration") += userJSONObject.getDouble("user_app_sum_duration")

        val user_app_use_bert_vec = userJSONObject.getObject("user_app_use_bert_vec", classOf[Vector])
        //        val (user_app_use_bert_vecIndices, user_app_use_bert_vecValues) = vec2TFSparseVec(user_app_use_bert_vec, sparseVectorNum)
        featureMap("user_app_use_bert_vec_indices") ++= user_app_use_bert_vec.toDense.getIndices
        featureMap("user_app_use_bert_vec_values") ++= user_app_use_bert_vec.toDense.getValues.map(_.toDouble)
        val user_appuse_positive_confidence = userJSONObject.getObject("user_appuse_positive_confidence", classOf[Vector])
        val (user_appuse_positive_confidenceIndices, user_appuse_positive_confidenceValues) = vec2TFSparseVec(user_appuse_positive_confidence)
        featureMap("user_appuse_positive_confidence_indices") ++= user_appuse_positive_confidenceIndices
        featureMap("user_appuse_positive_confidence_values") ++= user_appuse_positive_confidenceValues


        val user_appuse_positive_lift = userJSONObject.getObject("user_appuse_positive_lift", classOf[Vector])
        val (user_appuse_positive_liftIndices, user_appuse_positive_liftValues) = vec2TFSparseVec(user_appuse_positive_lift)
        featureMap("user_appuse_positive_lift_indices") ++= user_appuse_positive_liftIndices
        featureMap("user_appuse_positive_lift_values") ++= user_appuse_positive_liftValues
        val user_appuse_negtive_confidence = userJSONObject.getObject("user_appuse_negtive_confidence", classOf[Vector])
        val (user_appuse_negtive_confidenceIndices, user_appuse_negtive_confidenceValues) = vec2TFSparseVec(user_appuse_negtive_confidence)
        featureMap("user_appuse_negtive_confidence_indices") ++= user_appuse_negtive_confidenceIndices
        featureMap("user_appuse_negtive_confidence_values") ++= user_appuse_negtive_confidenceValues

        val user_appuse_negtive_lift = userJSONObject.getObject("user_appuse_negtive_lift", classOf[Vector])
        val (user_appuse_negtive_liftIndices, user_appuse_negtive_liftValues) = vec2TFSparseVec(user_appuse_negtive_lift)
        featureMap("user_appuse_negtive_lift_indices") ++= user_appuse_negtive_liftIndices
        featureMap("user_appuse_negtive_lift_values") ++= user_appuse_negtive_liftValues
        featureMap("user_bs_show_cnt_30d") += userJSONObject.getDouble("user_bs_show_cnt_30d")
        featureMap("user_bs_click_cnt_30d") += userJSONObject.getDouble("user_bs_click_cnt_30d")
        val user_bs_ctr_30d = CTR.calculate_news_ctr(userJSONObject.getDouble("user_bs_show_cnt_30d"), userJSONObject.getDouble("user_bs_click_cnt_30d")).toDouble
        featureMap("user_bs_ctr_30d") += user_bs_ctr_30d

        featureMap("user_bs_read_duration_30d") += "%.2f".format(userJSONObject.getDouble("user_bs_read_duration_30d") / 1000).toDouble
        featureMap("user_bs_first_show_day") += userJSONObject.getDouble("user_bs_first_show_day")
        featureMap("user_bs_first_click_day") += userJSONObject.getDouble("user_bs_first_click_day")
        featureMap("user_sb_show_cnt_30d") += userJSONObject.getDouble("user_sb_show_cnt_30d")
        featureMap("user_sb_click_cnt_30d") += userJSONObject.getDouble("user_sb_click_cnt_30d")
        val user_sb_ctr_30d = CTR.calculate_news_ctr(userJSONObject.getDouble("user_sb_show_cnt_30d"), userJSONObject.getDouble("user_sb_click_cnt_30d")).toDouble
        featureMap("user_sb_ctr_30d") += user_sb_ctr_30d
        featureMap("user_sb_read_duration_30d") += "%.2f".format(userJSONObject.getDouble("user_sb_read_duration_30d") / 1000).toDouble
        featureMap("user_sb_first_show_day") += userJSONObject.getDouble("user_sb_first_show_day")
        featureMap("user_sb_first_click_day") += userJSONObject.getDouble("user_sb_first_click_day")
        featureMap("user_wp_show_cnt_30d") += userJSONObject.getDouble("user_wp_show_cnt_30d")
        featureMap("user_wp_click_cnt_30d") += userJSONObject.getDouble("user_wp_click_cnt_30d")


        val user_wp_ctr_30d = CTR.calculate_news_ctr(userJSONObject.getDouble("user_wp_show_cnt_30d"), userJSONObject.getDouble("user_wp_click_cnt_30d")).toDouble
        featureMap("user_wp_ctr_30d") += user_wp_ctr_30d
        featureMap("user_wp_read_duration_30d") += "%.2f".format(userJSONObject.getDouble("user_wp_read_duration_30d") / 1000).toDouble
        featureMap("user_wp_first_show_day") += userJSONObject.getDouble("user_wp_first_show_day")
        featureMap("user_wp_first_click_day") += userJSONObject.getDouble("user_wp_first_click_day")
        featureMap("user_gs_show_cnt_30d") += userJSONObject.getDouble("user_gs_show_cnt_30d")
        featureMap("user_gs_click_cnt_30d") += userJSONObject.getDouble("user_gs_click_cnt_30d")
        val user_gs_ctr_30d = CTR.calculate_news_ctr(userJSONObject.getDouble("user_gs_show_cnt_30d"), userJSONObject.getDouble("user_gs_click_cnt_30d")).toDouble
        featureMap("user_gs_ctr_30d") += user_gs_ctr_30d
        featureMap("user_gs_read_duration_30d") += "%.2f".format(userJSONObject.getDouble("user_gs_read_duration_30d") / 1000.0).toDouble
        featureMap("user_gs_first_show_day") += userJSONObject.getDouble("user_gs_first_show_day")
        featureMap("user_gs_first_click_day") += userJSONObject.getDouble("user_gs_first_click_day")
        featureMap("user_wt_show_cnt_30d") += userJSONObject.getDouble("user_wt_show_cnt_30d")
        featureMap("user_wt_click_cnt_30d") += userJSONObject.getDouble("user_wt_click_cnt_30d")
        val user_wt_ctr_30d = CTR.calculate_news_ctr(userJSONObject.getDouble("user_wt_show_cnt_30d"), userJSONObject.getDouble("user_wt_click_cnt_30d")).toDouble
        featureMap("user_wt_ctr_30d") += user_wt_ctr_30d
        featureMap("user_wt_read_duration_30d") += "%.2f".format(userJSONObject.getDouble("user_wt_read_duration_30d") / 1000.0).toDouble
        featureMap("user_wt_first_show_day") += userJSONObject.getDouble("user_wt_first_show_day")
        featureMap("user_wt_first_click_day") += userJSONObject.getDouble("user_wt_first_click_day")
        featureMap("user_is_marry") += userJSONObject.getInteger("user_is_marry")
        featureMap("user_career_first") += userJSONObject.getInteger("user_career_first")
        featureMap("user_career_sec") += userJSONObject.getInteger("user_career_sec")
        featureMap("user_life_stage") += userJSONObject.getInteger("user_life_stage")
        featureMap("user_is_has_baby") += userJSONObject.getInteger("user_is_has_baby")
        featureMap("user_house_type") += userJSONObject.getInteger("user_house_type")
        featureMap("user_house_price") += userJSONObject.getDouble("user_house_price")
        val user_smart_launcher_feed_news_duration_fc_read_duration = Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_duration_fc_read_duration")
        val durationSequence = Tools.getDurationSequence(user_smart_launcher_feed_news_duration_fc_read_duration, eventTime, intervals)
        featureMap("sb_user_r_read_duration") += "%.2f".format(durationSequence(0) / 1000.0).toDouble
        featureMap("sb_user_l_read_duration") += "%.2f".format(durationSequence(1) / 1000.0).toDouble
        featureMap("sb_user_1hour_read_duration") += "%.2f".format(durationSequence(2) / 1000.0).toDouble
        featureMap("sb_user_48hour_read_duration") += "%.2f".format(durationSequence(3) / 1000.0).toDouble
        featureMap("sb_user_7days_read_duration") += "%.2f".format(durationSequence(4) / 1000.0).toDouble
        featureMap("sb_user_14days_read_duration") += "%.2f".format(durationSequence(5) / 1000.0).toDouble


        val user_smart_launcher_feed_news_duration_fc_read_durationFill = user_smart_launcher_feed_news_duration_fc_read_duration.getT2.map(x =>
          if (x.contains("{")) {
            JSON.parseObject(x).getString("duration").toDouble
          } else {
            x.toDouble
          }
        ).map(duration => "%.2f".format(duration / 1000.0).toDouble) ++ new Array[Double](300 - userSmartLauncherFeedNewsClickFcNewsTopCategoryV3.length)
        featureMap("user_smart_launcher_feed_news_duration_fc_read_duration") ++= user_smart_launcher_feed_news_duration_fc_read_durationFill
        val showNewsSourceLevel1History = Tools.getHistoryVector(userSmartLauncherFeedNewsShowFcNewsSourceLevel1, 20)
        val (user_show_news_source_level1_history_indices, user_show_news_source_level1_history_values) = vec2TFSparseVec(showNewsSourceLevel1History)
        featureMap("user_show_news_source_level1_history_indices") ++= user_show_news_source_level1_history_indices
        featureMap("user_show_news_source_level1_history_values") ++= user_show_news_source_level1_history_values
        val clickNewsSourceLevel1History = Tools.getHistoryVector(userSmartLauncherFeedNewsClickFcNewsSourceLevel1, 20)
        val (user_click_news_source_level1_history_indices, user_click_news_source_level1_history_values) = vec2TFSparseVec(clickNewsSourceLevel1History)
        featureMap("user_click_news_source_level1_history_indices") ++= user_click_news_source_level1_history_indices
        featureMap("user_click_news_source_level1_history_values") ++= user_click_news_source_level1_history_values
        val showNewsSourceLevel2History = Tools.getHistoryVector(userSmartLauncherFeedNewsShowFcNewsSourceLevel2, 50)
        val (user_show_news_source_level2_history_indices, user_show_news_source_level2_history_values) = vec2TFSparseVec(showNewsSourceLevel2History)
        featureMap("user_show_news_source_level2_history_indices") ++= user_show_news_source_level2_history_indices
        featureMap("user_show_news_source_level2_history_values") ++= user_show_news_source_level2_history_values
        val clickNewsSourceLevel2History = Tools.getHistoryVector(userSmartLauncherFeedNewsClickFcNewsSourceLevel2, 50)
        val (user_click_news_source_level2_history_indices, user_click_news_source_level2_history_values) = vec2TFSparseVec(clickNewsSourceLevel2History)
        featureMap("user_click_news_source_level2_history_indices") ++= user_click_news_source_level2_history_indices
        featureMap("user_click_news_source_level2_history_values") ++= user_click_news_source_level2_history_values
        val showNewsTopCategoryV3History = Tools.getHistoryVector(userSmartLauncherFeedNewsShowFcNewsTopCategoryV3, 24)
        val (user_show_news_top_category_v3_history_indices, user_show_news_top_category_v3_history_values) = vec2TFSparseVec(showNewsTopCategoryV3History)


        featureMap("user_show_news_top_category_v3_history_indices") ++= user_show_news_top_category_v3_history_indices
        featureMap("user_show_news_top_category_v3_history_values") ++= user_show_news_top_category_v3_history_values
        val clickNewsTopCategoryV3History = Tools.getHistoryVector(userSmartLauncherFeedNewsClickFcNewsTopCategoryV3, 24)
        val (user_click_news_top_category_v3_history_indices, user_click_news_top_category_v3_history_values) = vec2TFSparseVec(clickNewsTopCategoryV3History)
        featureMap("user_click_news_top_category_v3_history_indices") ++= user_click_news_top_category_v3_history_indices
        featureMap("user_click_news_top_category_v3_history_values") ++= user_click_news_top_category_v3_history_values
        val showNewsSubCategoryV3History = Tools.getHistoryVector(userSmartLauncherFeedNewsShowFcNewsSubCategoryV3, 132)
        val (user_show_news_sub_category_v3_history_indices, user_show_news_sub_category_v3_history_values) = vec2TFSparseVec(showNewsSubCategoryV3History)
        featureMap("user_show_news_sub_category_v3_history_indices") ++= user_show_news_sub_category_v3_history_indices
        featureMap("user_show_news_sub_category_v3_history_values") ++= user_show_news_sub_category_v3_history_values
        val clickNewsSubCategoryV3History = Tools.getHistoryVector(userSmartLauncherFeedNewsClickFcNewsSubCategoryV3, 132)
        val (user_click_news_sub_category_v3_history_indices, user_click_news_sub_category_v3_history_values) = vec2TFSparseVec(clickNewsSubCategoryV3History)
        featureMap("user_click_news_sub_category_v3_history_indices") ++= user_click_news_sub_category_v3_history_indices
        featureMap("user_click_news_sub_category_v3_history_values") ++= user_click_news_sub_category_v3_history_values
        val showNewsContentTagTop5History = Tools.getHistoryVector(userSmartLauncherFeedNewsShowFcNewsContentTagTop5, 500000, 0)
        val (user_show_news_content_tag_vec_top5_history_indices, user_show_news_content_tag_vec_top5_history_values) = vec2TFSparseVec(showNewsContentTagTop5History)
        featureMap("user_show_news_content_tag_vec_top5_history_indices") ++= user_show_news_content_tag_vec_top5_history_indices
        featureMap("user_show_news_content_tag_vec_top5_history_values") ++= user_show_news_content_tag_vec_top5_history_values
        val clickNewsContentTagTop5History = Tools.getHistoryVector(userSmartLauncherFeedNewsClickFcNewsContentTagTop5, 500000, 0)
        val (user_click_news_content_tag_vec_top5_history_indices, user_click_news_content_tag_vec_top5_history_values) = vec2TFSparseVec(clickNewsContentTagTop5History)
        featureMap("user_click_news_content_tag_vec_top5_history_indices") ++= user_click_news_content_tag_vec_top5_history_indices
        featureMap("user_click_news_content_tag_vec_top5_history_values") ++= user_click_news_content_tag_vec_top5_history_values
        val showNewsTitleTagTop5History = Tools.getHistoryVector(userSmartLauncherFeedNewsShowFcNewsTitleTagTop5, 500000, 0)
        val (user_show_news_title_tag_vec_top5_history_indices, user_show_news_title_tag_vec_top5_history_values) = vec2TFSparseVec(showNewsTitleTagTop5History)


        featureMap("user_show_news_title_tag_vec_top5_history_indices") ++= user_show_news_title_tag_vec_top5_history_indices
        featureMap("user_show_news_title_tag_vec_top5_history_values") ++= user_show_news_title_tag_vec_top5_history_values
        val clickNewsTitleTagTop5History = Tools.getHistoryVector(userSmartLauncherFeedNewsClickFcNewsTitleTagTop5, 500000, 0)
        val (user_click_news_title_tag_vec_top5_history_indices, user_click_news_title_tag_vec_top5_history_values) = vec2TFSparseVec(clickNewsTitleTagTop5History)
        featureMap("user_click_news_title_tag_vec_top5_history_indices") ++= user_click_news_title_tag_vec_top5_history_indices
        featureMap("user_click_news_title_tag_vec_top5_history_values") ++= user_click_news_title_tag_vec_top5_history_values
        val showNewsTopicTop5History = Tools.getHistoryVector(userSmartLauncherFeedNewsShowFcNewsTopicTop5, 2001, 2000)
        val (user_show_news_topic_vec_top5_history_indices, user_show_news_topic_vec_top5_history_values) = Tools.vec2TFSparseVec(showNewsTopicTop5History, defaultTopicVec)
        featureMap("user_show_news_topic_vec_top5_history_indices") ++= user_show_news_topic_vec_top5_history_indices
        featureMap("user_show_news_topic_vec_top5_history_values") ++= user_show_news_topic_vec_top5_history_values
        val clickNewsTopicTop5History = Tools.getHistoryVector(userSmartLauncherFeedNewsClickFcNewsTopicTop5, 2001, 2000)
        val (user_click_news_topic_vec_top5_history_indices, user_click_news_topic_vec_top5_history_values) = Tools.vec2TFSparseVec(clickNewsTopicTop5History, defaultTopicVec)
        featureMap("user_click_news_topic_vec_top5_history_indices") ++= user_click_news_topic_vec_top5_history_indices
        featureMap("user_click_news_topic_vec_top5_history_values") ++= user_click_news_topic_vec_top5_history_values
        val user_rt_top_category_v3 = userJSONObject.getObject("user_rt_top_category_v3", classOf[Vector])
        val (user_rt_top_category_v3_indices, user_rt_top_category_v3_values) = vec2TFSparseVec(user_rt_top_category_v3)
        featureMap("user_rt_top_category_v3_indices") ++= user_rt_top_category_v3_indices
        featureMap("user_rt_top_category_v3_values") ++= user_rt_top_category_v3_values
        val user_rt_sub_category_v3 = userJSONObject.getObject("user_rt_sub_category_v3", classOf[Vector])
        val (user_rt_sub_category_v3_indices, user_rt_sub_category_v3_values) = vec2TFSparseVec(user_rt_sub_category_v3)
        featureMap("user_rt_sub_category_v3_indices") ++= user_rt_sub_category_v3_indices
        featureMap("user_rt_sub_category_v3_values") ++= user_rt_sub_category_v3_values
        val user_rt_tag = Tools.map2Vec(Tools.jsonGetMap(userJSONObject, "user_rt_tag"), 500000)
        featureMap("user_rt_tag_indices") ++= user_rt_tag.getIndices
        featureMap("user_rt_tag_values") ++= user_rt_tag.getValues.map(_.toDouble)
        val user_rt_title_tag = Tools.map2Vec(Tools.jsonGetMap(userJSONObject, "user_rt_title_tag"), 500000)
        featureMap("user_rt_title_tag_indices") ++= user_rt_title_tag.getIndices
        featureMap("user_rt_title_tag_values") ++= user_rt_title_tag.getValues.map(_.toDouble)
        val user_rt_topic = userJSONObject.getObject("user_rt_topic", classOf[Vector])
        val (user_rt_topic_indices, user_rt_topic_values) = Tools.vec2TFSparseVec(user_rt_topic, defaultTopicVec)
        featureMap("user_rt_topic_indices") ++= user_rt_topic_indices
        featureMap("user_rt_topic_values") ++= user_rt_topic_values


        val cosFeatureMap = Map(
          "cos_news_topcategory_v3_user_long" -> MLUtils.cos(userNewsLongTopCategoryV3, newsTopcategoryV3Vec),
          "cos_news_topcategory_v3_user_rt" -> MLUtils.cos(user_rt_top_category_v3, newsTopcategoryV3Vec),
          "cos_news_subcategory_v3_user_long" -> MLUtils.cos(user_news_long_sub_category_v3, newsSubcategoryV3Vec),
          "cos_news_subcategory_v3_user_rt" -> MLUtils.cos(user_rt_sub_category_v3, newsSubcategoryV3Vec),
          "cos_news_title_tag_user_long" -> MLUtils.cos(userNewsTitleTagPreferenceVec, newsTitleTagVec),
          "cos_news_title_tag_user_rt" -> MLUtils.cos(user_rt_title_tag, newsTitleTagVec),
          "cos_news_topic_vec_user_rt" -> MLUtils.cos(user_rt_topic, news_topic_vec)
        )
        cosFeatureMap.map { x =>
          featureMap(x._1) += x._2.toDouble
        }

        featureMap("news_rt_average_read_duration") += "%.2f".format(itemJSONObject.getDouble("news_rt_average_read_duration") / 1000).toDouble
        featureMap("news_rt_total_read_duration") += "%.2f".format(itemJSONObject.getDouble("news_rt_total_read_duration") / 1000).toDouble
        featureMap("news_rt_read_frequency") += itemJSONObject.getDouble("news_rt_read_frequency")
        featureMap("news_rt_click_frequency") += itemJSONObject.getDouble("news_rt_click_frequency")
        featureMap("news_rt_show_frequency") += itemJSONObject.getDouble("news_rt_show_frequency")
        featureMap("news_rt_ctr_frequency") += itemJSONObject.getDouble("news_rt_ctr") //news_rt_ctr_frequency这个特征的命名有点问题，但是先不做修改，
        featureMap("news_rt_repeat_recommend_frequency") += itemJSONObject.getDouble("news_rt_repeat_recommend_frequency")
        featureMap("news_rt_tai_shui_frequency") += itemJSONObject.getDouble("news_rt_tai_shui_frequency")
        featureMap("news_rt_la_hei_frequency") += itemJSONObject.getDouble("news_rt_la_hei_frequency")
        val news_rt_negative_top_category_v3 = itemJSONObject.getObject("news_rt_negative_top_category_v3", classOf[Vector])
        val (news_rt_negative_top_category_v3_indices, news_rt_negative_top_category_v3_values) = vec2TFSparseVec(news_rt_negative_top_category_v3)
        featureMap("news_rt_negative_top_category_v3_indices") ++= news_rt_negative_top_category_v3_indices
        featureMap("news_rt_negative_top_category_v3_values") ++= news_rt_negative_top_category_v3_values


        val news_rt_negative_tag_vec = itemJSONObject.getObject("news_rt_negative_tag_vec", classOf[Vector])
        val (news_rt_negative_tag_vec_indices, news_rt_negative_tag_vec_values) = vec2TFSparseVec(news_rt_negative_tag_vec)
        featureMap("news_rt_negative_tag_vec_indices") ++= news_rt_negative_tag_vec_indices
        featureMap("news_rt_negative_tag_vec_values") ++= news_rt_negative_tag_vec_values
        featureMap("news_rt_collect_frequency") += itemJSONObject.getDouble("news_rt_collect_frequency")
        featureMap("news_rt_share_frequency") += itemJSONObject.getDouble("news_rt_share_frequency")
        featureMap("news_rt_like_frequency") += itemJSONObject.getDouble("news_rt_like_frequency")
        featureMap("news_rt_comment_frequency") += itemJSONObject.getDouble("news_rt_comment_frequency")
        val recall_type = try (itemJSONObject.getString("recall_type").split("\\|")) catch {
          case e: Exception => Array("")
        }
        val encode_recall_type = recall_type.map { x =>
          val code = try {
            ContinuousEncoder.encode("feednews_recall_type", x, EncodeEnv.PRD)
          } catch {
            case e: Exception =>
              0
          }
          code
        }.filter(_ < 500)
        featureMap("recall_type") ++= encode_recall_type
        val news_source_encode = try {
          ContinuousEncoder.encode("news_source", news_source, EncodeEnv.PRD)
        } catch {
          case e: Exception =>
            0
        }
        featureMap("news_source_encode") += news_source_encode
        val source_pkg_name = userJSONObject.getString("source_pkg_name")
        val source_pkg_name_code = try {
          ContinuousEncoder.encode("source_pkg_name", source_pkg_name, EncodeEnv.PRD)
        } catch {
          case e: Exception =>
            0
        }
        featureMap("source_pkg_name_code") += source_pkg_name_code


        ///新闻源画像
        featureMap("news_source_cnt_all_pub") += itemJSONObject.getDouble("news_source_cnt_all_pub")
        featureMap("news_source_avg_clk") += itemJSONObject.getDouble("news_source_avg_clk")
        featureMap("news_source_avg_pv") += itemJSONObject.getDouble("news_source_avg_pv")
        featureMap("news_source_avg_ctr") += itemJSONObject.getDouble("news_source_avg_ctr")
        val news_source_cnt_top_pub_v3 = itemJSONObject.getObject("news_source_cnt_top_pub_v3", classOf[Vector])
        val (news_source_cnt_top_pub_v3_indices, news_source_cnt_top_pub_v3_values) = vec2TFSparseVec(news_source_cnt_top_pub_v3)
        featureMap("news_source_cnt_top_pub_v3_indices") ++= news_source_cnt_top_pub_v3_indices
        featureMap("news_source_cnt_top_pub_v3_values") ++= news_source_cnt_top_pub_v3_values
        val news_source_cnt_sub_pub_v3 = itemJSONObject.getObject("news_source_cnt_sub_pub_v3", classOf[Vector])
        val (news_source_cnt_sub_pub_v3_indices, news_source_cnt_sub_pub_v3_values) = vec2TFSparseVec(news_source_cnt_sub_pub_v3)
        featureMap("news_source_cnt_sub_pub_v3_indices") ++= news_source_cnt_sub_pub_v3_indices
        featureMap("news_source_cnt_sub_pub_v3_values") ++= news_source_cnt_sub_pub_v3_values
        val news_source_clk_top_v3 = itemJSONObject.getObject("news_source_clk_top_v3", classOf[Vector])
        val (news_source_clk_top_v3_indices, news_source_clk_top_v3_values) = vec2TFSparseVec(news_source_clk_top_v3)
        featureMap("news_source_clk_top_v3_indices") ++= news_source_clk_top_v3_indices
        featureMap("news_source_clk_top_v3_values") ++= news_source_clk_top_v3_values
        val news_source_pv_top_v3 = itemJSONObject.getObject("news_source_pv_top_v3", classOf[Vector])
        val (news_source_pv_top_v3_indices, news_source_pv_top_v3_values) = vec2TFSparseVec(news_source_pv_top_v3)
        featureMap("news_source_pv_top_v3_indices") ++= news_source_pv_top_v3_indices
        featureMap("news_source_pv_top_v3_values") ++= news_source_pv_top_v3_values
        val news_source_ctr_top_v3 = itemJSONObject.getObject("news_source_ctr_top_v3", classOf[Vector])
        val (news_source_ctr_top_v3_indices, news_source_ctr_top_v3_values) = vec2TFSparseVec(news_source_ctr_top_v3)
        featureMap("news_source_ctr_top_v3_indices") ++= news_source_ctr_top_v3_indices
        featureMap("news_source_ctr_top_v3_values") ++= news_source_ctr_top_v3_values
        val news_source_clk_sub_v3 = itemJSONObject.getObject("news_source_clk_sub_v3", classOf[Vector])


        val (news_source_clk_sub_v3_indices, news_source_clk_sub_v3_values) = vec2TFSparseVec(news_source_clk_sub_v3)
        featureMap("news_source_clk_sub_v3_indices") ++= news_source_clk_sub_v3_indices
        featureMap("news_source_clk_sub_v3_values") ++= news_source_clk_sub_v3_values
        val news_source_pv_sub_v3 = itemJSONObject.getObject("news_source_pv_sub_v3", classOf[Vector])
        val (news_source_pv_sub_v3_indices, news_source_pv_sub_v3_values) = vec2TFSparseVec(news_source_pv_sub_v3)
        featureMap("news_source_pv_sub_v3_indices") ++= news_source_pv_sub_v3_indices
        featureMap("news_source_pv_sub_v3_values") ++= news_source_pv_sub_v3_values
        val news_source_ctr_sub_v3 = itemJSONObject.getObject("news_source_ctr_sub_v3", classOf[Vector])
        val (news_source_ctr_sub_v3_indices, news_source_ctr_sub_v3_values) = vec2TFSparseVec(news_source_ctr_sub_v3)
        featureMap("news_source_ctr_sub_v3_indices") ++= news_source_ctr_sub_v3_indices
        featureMap("news_source_ctr_sub_v3_values") ++= news_source_ctr_sub_v3_values

        // 添加用户侧特征original_user_news_long_tag_vec原始数据特征 2020-07-17
        val originalUserNewsLongTag = if (Tools.jsonGetStringMap(userJSONObject, "original_user_news_long_tag").isEmpty) Map("" -> 0.0) else Tools.jsonGetStringMap(userJSONObject, "original_user_news_long_tag")
        val userOriginalNewsLongTagHashArr = originalUserNewsLongTag.toArray.map { x => (Math.abs(x._1.hashCode % 10000000), x._2) }.groupBy(_._1).mapValues(_.map(_._2).sum).toArray.sortBy(_._1)
        val userOriginalNewsLongTagVec = Vector.builder(10000000).index(userOriginalNewsLongTagHashArr.map(_._1)).value(userOriginalNewsLongTagHashArr.map(_._2.toFloat)).build()
        val (userOriginalNewsLongTagVecIndices, userOriginalNewsLongTagVecValues) = vec2TFSparseVec(userOriginalNewsLongTagVec)
        featureMap("original_user_news_long_tag_vec_indices") ++= userOriginalNewsLongTagVecIndices
        featureMap("original_user_news_long_tag_vec_values") ++= userOriginalNewsLongTagVecValues
        // 添加用户侧特征original_user_news_long_tag_vec原始数据特征 2020-07-17

        // 添加物料侧特征original_news_tag_vec原始数据特征 2020-07-17
        val newsOriginalTagVecMap = if (Tools.jsonGetStringMap(itemJSONObject, "original_news_tag_vec").isEmpty) Map("" -> 0.0) else Tools.jsonGetStringMap(itemJSONObject, "original_news_tag_vec")
        val newsOriginalTagVecHashArr = newsOriginalTagVecMap.toArray.map { x => (Math.abs(x._1.hashCode % 10000000), x._2) }.groupBy(_._1).mapValues(_.map(_._2).sum).toArray.sortBy(_._1)
        val newsOriginalTagVec = Vector.builder(10000000).index(newsOriginalTagVecHashArr.map(_._1)).value(newsOriginalTagVecHashArr.map(_._2.toFloat)).build()
        val (newsOriginalTagVecIndices, newsOriginalTagVecValues) = vec2TFSparseVec(newsOriginalTagVec)
        featureMap("original_news_tag_vec_indices") ++= newsOriginalTagVecIndices
        featureMap("original_news_tag_vec_values") ++= newsOriginalTagVecValues
        // 添加物料侧特征original_news_tag_vec原始数据特征 2020-07-17


        // 添加用户侧特征original_user_query_long_tag_vec原始数据特征 2020-07-17
        val userOriginalQueryLongTagVecMap = if (Tools.jsonGetStringMap(userJSONObject, "original_user_query_long_tag").isEmpty) Map("" -> 0.0) else Tools.jsonGetStringMap(userJSONObject, "original_user_query_long_tag")
        val userOriginalQueryLongTagVecArr = userOriginalQueryLongTagVecMap.toArray.map(value => (Math.abs(value._1.hashCode % 10000000), value._2)).groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val userOriginalQueryLongTagVec = Vector.builder(10000000).index(userOriginalQueryLongTagVecArr.map(_._1)).value(userOriginalQueryLongTagVecArr.map(_._2.toFloat)).build()
        val (userOriginalQueryLongTagVecIndices, userOriginalQueryLongTagVecValues) = vec2TFSparseVec(userOriginalQueryLongTagVec)
        featureMap("original_user_query_long_tag_indices") ++= userOriginalQueryLongTagVecIndices
        featureMap("original_user_query_long_tag_values") ++= userOriginalQueryLongTagVecValues
        // 添加用户侧特征original_user_query_long_tag_vec原始数据特征 2020-07-17

        // 添加用户侧特征original_user_news_title_tag_preference_vec原始数据特征 2020-07-17
        val userOriginalNewsTitleTagPreferenceMap = if (Tools.jsonGetStringMap(userJSONObject, "original_user_news_title_tag_preference").isEmpty) Map("" -> 0.0) else Tools.jsonGetStringMap(userJSONObject, "original_user_news_title_tag_preference")
        val userOriginalNewsTitleTagPreferenceArr: Array[(Int, Double)] = userOriginalNewsTitleTagPreferenceMap.toArray.map(value => (Math.abs(value._1.hashCode % 10000000), value._2)).groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val userOriginalNewsTitleTagPreferenceVec = Vector.builder(10000000).index(userOriginalNewsTitleTagPreferenceArr.map(_._1)).value(userOriginalNewsTitleTagPreferenceArr.map(_._2.toFloat)).build()
        val (userOriginalNewsTitleTagPreferenceIndices, userOriginalNewsTitleTagPreferenceValues) = vec2TFSparseVec(userOriginalNewsTitleTagPreferenceVec)
        featureMap("original_user_news_title_tag_preference_indices") ++= userOriginalNewsTitleTagPreferenceIndices
        featureMap("original_user_news_title_tag_preference_values") ++= userOriginalNewsTitleTagPreferenceValues
        // 添加用户侧特征original_user_news_title_tag_preference_vec原始数据特征 2020-07-17

        // 添加物料侧特征original_news_title_tag_vec原始数据特征 2020-07-17
        val newsOriginalTitleTagVecMap = if (Tools.jsonGetStringMap(itemJSONObject, "original_news_title_tag_vec").isEmpty) Map("" -> 0.0) else Tools.jsonGetStringMap(itemJSONObject, "original_news_title_tag_vec")
        val newsOriginalTitleTagVecArr: Array[(Int, Double)] = newsOriginalTitleTagVecMap.toArray.map(value => (Math.abs(value._1.hashCode % 10000000), value._2)).groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val newsOriginalTitleTagVec = Vector.builder(10000000).index(newsOriginalTitleTagVecArr.map(_._1)).value(newsOriginalTitleTagVecArr.map(_._2.toFloat)).build()
        val (newsOriginalTitleTagVecIndices, newsOriginalTitleTagVecValues) = vec2TFSparseVec(newsOriginalTitleTagVec)
        featureMap("original_news_title_tag_vec_indices") ++= newsOriginalTitleTagVecIndices
        featureMap("original_news_title_tag_vec_values") ++= newsOriginalTitleTagVecValues
        // 添加物料侧特征original_news_title_tag_vec原始数据特征 2020-07-17


        // 添加物料侧特征original_news_content_tag_vec原始数据特征 2020-07-17
        val newsOriginalContentTagVecMap = if (Tools.jsonGetStringMap(itemJSONObject, "original_news_content_tag_vec").isEmpty) Map("" -> 0.0) else Tools.jsonGetStringMap(itemJSONObject, "original_news_content_tag_vec")
        val newsOriginalContentTagVecArr: Array[(Int, Double)] = newsOriginalContentTagVecMap.toArray.map(value => (Math.abs(value._1.hashCode % 10000000), value._2)).groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val newsOriginalContentTagVec = Vector.builder(10000000).index(newsOriginalContentTagVecArr.map(_._1)).value(newsOriginalContentTagVecArr.map(_._2.toFloat)).build()
        val (newsOriginalContentTagVecIndices, newsOriginalContentTagVecValues) = vec2TFSparseVec(newsOriginalContentTagVec)
        featureMap("original_news_content_tag_vec_indices") ++= newsOriginalContentTagVecIndices
        featureMap("original_news_content_tag_vec_values") ++= newsOriginalContentTagVecValues
        // 添加物料侧特征original_news_content_tag_vec原始数据特征 2020-07-17

        // 添加物料侧特征original_news_core_tag_vec原始数据特征 2020-07-17
        val newsOriginalCoreTagVecMap = if (Tools.jsonGetStringMap(itemJSONObject, "original_news_core_tag_vec").isEmpty) Map("" -> 0.0) else Tools.jsonGetStringMap(itemJSONObject, "original_news_core_tag_vec")
        val newsOriginalCoreTagVecArr: Array[(Int, Double)] = newsOriginalCoreTagVecMap.toArray.map(value => (Math.abs(value._1.hashCode % 10000000), value._2)).groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum)).toArray.sortBy(_._1)
        val newsOriginalCoreTagVec = Vector.builder(10000000).index(newsOriginalCoreTagVecArr.map(_._1)).value(newsOriginalCoreTagVecArr.map(_._2.toFloat)).build()
        val (newsOriginalCoreTagVecIndices, newsOriginalCoreTagVecValues) = vec2TFSparseVec(newsOriginalCoreTagVec)
        featureMap("original_news_core_tag_vec_indices") ++= newsOriginalCoreTagVecIndices
        featureMap("original_news_core_tag_vec_values") ++= newsOriginalCoreTagVecValues

        // ************添加用户画像 端侧特征  2020-08-11 start************
        featureMap("continuous_clicked") += userJSONObject.getDouble("continuous_clicked")
        featureMap("continuous_request") += userJSONObject.getDouble("continuous_request")
        // ************添加用户画像 端侧特征  2020-08-11 end************

        // ************添加物料侧新特征 2020-08-24 start************

        val newsTopCategoryV3Update: Vector = itemJSONObject.getObject("top_category_v3_update", classOf[Vector])
        val (newsTopCategoryV3UpdateIndices, newsTopCategoryV3UpdateValues) = vec2TFSparseVec(newsTopCategoryV3Update)
        featureMap("news_top_category_v3_update_indices") ++= newsTopCategoryV3UpdateIndices
        featureMap("news_top_category_v3_update_values") ++= newsTopCategoryV3UpdateValues


        val newsSubCategoryV3Update: Vector = itemJSONObject.getObject("sub_category_v3_update", classOf[Vector])
        val (newsSubCategoryV3UpdateIndices, newsSubCategoryV3UpdateValues) = vec2TFSparseVec(newsSubCategoryV3Update)
        featureMap("news_sub_category_v3_update_indices") ++= newsSubCategoryV3UpdateIndices
        featureMap("news_sub_category_v3_update_values") ++= newsSubCategoryV3UpdateValues

        val thirdCategoryV3Update: Vector = itemJSONObject.getObject("third_category_v3_update", classOf[Vector])
        val (thirdCategoryV3UpdateIndices, thirdCategoryV3UpdateValues) = vec2TFSparseVec(thirdCategoryV3Update)
        featureMap("news_third_category_v3_update_indices") ++= thirdCategoryV3UpdateIndices
        featureMap("news_third_category_v3_update_values") ++= thirdCategoryV3UpdateValues

        // ************添加物料侧新特征 2020-08-24 end************

        // ************添加用户侧新特征 2020-09-03 start************
        featureMap("yestoday_is_holiday") += userJSONObject.getInteger("yestoday_is_holiday")
        featureMap("tomorrow_is_holiday") += userJSONObject.getInteger("tomorrow_is_holiday")
        // ************添加用户侧新特征 2020-08-03 end************

        // ************添加用户侧新特征 2020-09-10 start************
        val appsDurationVector: Vector = userJSONObject.getObject("apps_duration", classOf[Vector])
        val (appsDurationVectorIndices, appsDurationVectorValues) = vec2TFSparseVec(appsDurationVector)
        featureMap("appsDurationVectorIndices") ++= appsDurationVectorIndices
        featureMap("appsDurationVectorValues") ++= appsDurationVectorValues
        // ************添加用户侧新特征 2020-09-10 end************

        //************2020-09-29 添加曝光点击序列特征************
        val userSmartLauncherFeedNewsShowFcActionFlag = Tools.findClickInShow(Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_show_fc_news_id").getT2, Tools.getTuple2(userJSONObject, "user_smart_launcher_feed_news_click_fc_news_id").getT2)
        val userSmartLauncherFeedNewsShowFcActionFlagFill = userSmartLauncherFeedNewsShowFcActionFlag ++ new Array[Int](300 - userSmartLauncherFeedNewsShowFcActionFlag.length)
        featureMap("user_smart_launcher_feed_news_show_fc_action_flag") ++= userSmartLauncherFeedNewsShowFcActionFlagFill
        featureMap("user_smart_launcher_feed_news_show_fc_action_flag_length") += userSmartLauncherFeedNewsShowFcActionFlag.length
        //************2020-09-29 添加曝光点击序列特征************


        //************2020-11-19 添加正负反馈特征************
        featureMap("feedback_negative_flag") += itemJSONObject.getInteger("feedback_negative_flag")
        featureMap("feedback_like_flag") += itemJSONObject.getInteger("feedback_like_flag")
        featureMap("feedback_comment_flag") += itemJSONObject.getInteger("feedback_comment_flag")
        featureMap("feedback_collect_flag") += itemJSONObject.getInteger("feedback_collect_flag")
        featureMap("feedback_share_flag") += itemJSONObject.getInteger("feedback_share_flag")
        //************2020-09-19 添加正负反馈特征************

        sparseVectorNum += 1
    }
    Row(featureMap.map(_._2).toArray: _*)
  }

  def handlePartition(partiton: Iterator[(JSONObject, JSONObject)], preBatch: Int): Iterator[Row] = {
    partiton.sliding(preBatch, preBatch).filter(_.size == preBatch).map(handleBatch)
  }

  def toTFrecord(spark: SparkSession, rdd: RDD[(JSONObject, JSONObject)], preBatch: Int, filePath: String, repartition: Int) = {
    val preBatchedDF = toDF(spark, rdd, preBatch, repartition)
    writeTFRecord(preBatchedDF, filePath)
    preBatchedDF
  }

  def writeTFRecord(df: DataFrame, filePath: String): Unit = {
    df.write.mode(SaveMode.Overwrite).format("tfrecords").option("recordType", "Example").save(filePath)
  }

  def toDF(spark: SparkSession, rdd: RDD[(JSONObject, JSONObject)], preBatch: Int, repartition: Int): DataFrame = {
    val preBatchedRDD = rdd.repartition(repartition).mapPartitions(handlePartition(_, preBatch))
    val featureList = feature.map { case (key, dateType) =>
      if (dateType == "IntegerType") {
        StructField(s"$key", ArrayType(IntegerType, true))
      } else if (dateType == "DoubleType") {
        StructField(s"$key", ArrayType(DoubleType, true))
      } else {
        StructField(s"$key", ArrayType(StringType, true))
      }
    }
    var f = feature.map(_._1)
    var i = 0
    val featureListShortKey = featureList.map { structField =>
      val fea = StructField(f(i), structField.dataType)
      i += 1
      fea
    }
    val schema = StructType(featureListShortKey)
    spark.createDataFrame(preBatchedRDD, schema)
  }
}

