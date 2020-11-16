//package recall
//
//
///**
//  * -- User: WanruXie
//  * -- Date: 2020/03/03
//  * -- Time: 17.00
//  * -- Des:给用户按周期内点击过的新闻源通过bert向量召回新闻存入redis
//  */
//
//import java.text.SimpleDateFormat
//import java.util.{Calendar, Date}
//import xyz.vivo.ai.util.redis.{RedisKeys, RedisPool}
//import org.apache.spark.sql.SparkSession
//
//
//
//object feednews_recall_by_bert {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("feednews_recall_by_bert").getOrCreate()
//    import spark.implicits._
//
//    val day = get_target_date(0)
//    //val h = (get_hour().toInt/6).floor.toInt+1
//    val last_week = get_target_date(0)
//    val current_hour = get_hour().toInt
//
//    val day_news = spark.sql("show partitions ai_smart_launcher.dm_m_news_bert_recalled_hourly_new").map(s => (s.getString(0).split("/")(0).split("=")(1),s.getString(0).split("/")(1).split("=")(1))).rdd.max()._1
//    val hour_news = spark.sql("show partitions ai_smart_launcher.dm_m_news_bert_recalled_hourly_new").map(s => (s.getString(0).split("/")(0).split("=")(1),s.getString(0).split("/")(1).split("=")(1))).rdd.max()._2
//    //各类内取ctr得分最高的top300
//    val df_news=spark.sql(
//      s"""
//         |select imei,articletype, concat_ws(',',collect_set(recalled)) as ids from
//         |(select imei, news_id,
//         |row_number()over(partition by imei order by event_time desc ) as flag
//         |from ai_smartboard.dw_singlenews_click_hourly  where part_dt>= '${last_week}'
//         |)a
//         |join
//         |(select articleno as news_id, recalled, articletype
//         |from ai_smart_launcher.dm_m_news_bert_recalled_hourly_new
//         | where day= '${day_news}'
//         | and hour='${hour_news}'
//         | )b on a.news_id = b.news_id where a.flag<=20
//         |group by imei,articletype
//       """.stripMargin)
//    val df_final_result = df_news.map{
//      row=>
//        val imei = row.getAs[String]("imei")
//        val articletype = row.getAs[String]("articletype")
//        val news_list = row.getAs[String]("ids").split(",").toSet.mkString(",")
//        (imei, articletype, news_list)
//    }.toDF("imei","articletype","ids")
//
//
//    df_final_result.repartition(50).foreachPartition {
//      partitionOfRows =>
//        val ai_feed_userprofile_prd = new RedisPool("ai-feed-userprofile-prd4.redis.dba.vivo.lan:11211,ai-feed-userprofile-prd3.redis.dba.vivo.lan:11212,ai-feed-userprofile-prd2.redis.dba.vivo.lan:11211,ai-feed-userprofile-prd1.redis.dba.vivo.lan:11211,ai-feed-userprofile-prd0.redis.dba.vivo.lan:11211").redis
//        partitionOfRows.foreach {
//          row =>
//            val imei = row.getAs[String]("imei")
//            val articletype = row.getAs[String]("articletype")
//            val news_list = row.getAs[String]("ids").split(",").toSet.mkString(",")
//            if(Set("511375721700019","864973049978615").contains(imei)) {
//              ai_feed_userprofile_prd.setex(s"{$imei}${articletype}:bert", 24 * 60 * 60, news_list)
//            }
//        }
//    }
//    //println(s"date: '${day}' hour_block: '${h}', '${count}' imeis for feednews of-recall-by-sourcecn to redis successed.")
//    df_news.createTempView("tmp1")
//
//    val res_table="ai_smart_launcher.dm_feednews_recall_result_by_bert"
//    spark.sql(s"INSERT OVERWRITE TABLE ${res_table} PARTITION (day='${day}',hour='${current_hour}') SELECT * FROM table")
//
//
//  }
//  def get_target_date(dif:Int): String = {
//    var now: Date = new Date()
//    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
//    var cal:Calendar=Calendar.getInstance()
//    cal.add(Calendar.DATE,-dif)
//    var days_ago=dateFormat.format(cal.getTime())
//    days_ago
//  }
//
//  def get_hour(): String = {
//    var now: Date = new Date()
//    var dateFormat: SimpleDateFormat = new SimpleDateFormat("HH")
//    var hour = dateFormat.format(now)
//    hour}
//
//}
//
//
