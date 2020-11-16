package spark

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import util.HbaseUtil

import scala.collection.mutable.ArrayBuffer

object Spark_FromHiveSql_SinkHbase extends App {

  case class Time(id: String, year: String, month: String, day: String)

  val conf = new SparkConf()
  val hbase_table = "cdc_table"
  conf.setAppName("Spark").setMaster("local[2]")
  private val session: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
  val frame: DataFrame = session.sql("use hive_batch")
  val frame1: DataFrame = session.sql("select event_time,date_time,db_name,table_name,primary_key,params,action_type from cdc_table")


  private val describe: RDD[Row] = frame1.rdd

  describe.foreachPartition(partition => {
    val conn = HbaseUtil.getHBaseConn
    val ttl = 60l
    val arrayBuffer = new ArrayBuffer[String]()
    partition.foreach(data => {
      val event_time = data.getAs[String]("event_time")
      val date_time = data.getAs[String]("date_time")
      val db_name = data.getAs[String]("db_name")
      val table_name = data.getAs[String]("table_name")
      val primary_key = data.getAs[String]("primary_key")
      val params = data.getAs[String]("params")
      val action_type = data.getAs[String]("action_type")
      val tuple_element = primary_key + "|" + event_time

      arrayBuffer.+=(tuple_element)
      val longStrings: String = arrayBuffer.toArray.toString


      if (conn == null) {
        println("conn is null.") //在Worker节点的Executor中打印
      } else {
        //设置表名
        val tableName = TableName.valueOf(hbase_table)
        //获取表的连接
        val table = conn.getTable(tableName)

        try {
          //设定行键rowkey
          val put = new Put(Bytes.toBytes(event_time))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("param"), Bytes.toBytes(longStrings))

          //put.setTTL(ttl)
          table.put(put)
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          table.close()
        }

      }


    })
  })

  session.stop()
}
