package spark

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import util.HbaseUtil

object SparkSQLWithHive extends App {
  case class Time(id:String,year:String,month:String,day:String)

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
    partition.foreach(data => {
      val event_time = data.getAs[String]("event_time")
      val date_time = data.getAs[String]("date_time")
      val db_name = data.getAs[String]("db_name")
      val table_name = data.getAs[String]("table_name")
      val primary_key = data.getAs[String]("primary_key")
      val params = data.getAs[String]("params")
      val action_type = data.getAs[String]("action_type")
      println(event_time + "," + date_time + "," + db_name + "," + table_name + "," + primary_key + "," + params + "," + action_type)

      if (conn == null){
        println("conn is null.") //在Worker节点的Executor中打印
      } else {
        //设置表名
        val tableName = TableName.valueOf(hbase_table)
        //获取表的连接
        val table = conn.getTable(tableName)

        try {
          //设定行键rowkey
          val put = new Put(Bytes.toBytes(event_time))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("event_time"), Bytes.toBytes(event_time))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date_time"), Bytes.toBytes(date_time))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("db_name"), Bytes.toBytes(db_name))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("table_name"), Bytes.toBytes(table_name))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("primary_key"), Bytes.toBytes(primary_key))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("params"), Bytes.toBytes(params))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("action_type"), Bytes.toBytes(action_type))
          put.setTTL(ttl)
          table.put(put)
        }catch {
          case e: Exception => e.printStackTrace()
        } finally {
          table.close()
        }

      }


    })
  })

//  describe.foreach(data => {
//    val event_time = data.getAs[String]("event_time")
//    val date_time = data.getAs[String]("date_time")
//    val db_name = data.getAs[String]("db_name")
//    val table_name = data.getAs[String]("table_name")
//    val primary_key = data.getAs[String]("primary_key")
//    val params = data.getAs[String]("params")
//    val action_type = data.getAs[String]("action_type")
//    println(event_time + "," + date_time + "," + db_name + "," + table_name + "," + primary_key + "," + params + "," + action_type)
//
//    val conn = HbaseUtil.getHBaseConn
//
//    if (conn == null){
//      println("conn is null.") //在Worker节点的Executor中打印
//    } else {
//      //设置表名
//      val tableName = TableName.valueOf(hbase_table)
//      //获取表的连接
//      val table = conn.getTable(tableName)
//
//      try {
//        //设定行键rowkey
//        val put = new Put(Bytes.toBytes(event_time))
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("event_time"), Bytes.toBytes(event_time))
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date_time"), Bytes.toBytes(date_time))
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("db_name"), Bytes.toBytes(db_name))
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("table_name"), Bytes.toBytes(table_name))
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("primary_key"), Bytes.toBytes(primary_key))
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("params"), Bytes.toBytes(params))
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("action_type"), Bytes.toBytes(action_type))
//        table.put(put)
//      }catch {
//        case e: Exception => e.printStackTrace()
//      } finally {
//        table.close()
//      }
//
//    }
//  })

//  private val rdd: RDD[Row] = frame.toDF("imei","model","start_time","params").rdd
//  private val rdd2: RDD[(String, String, Long, collection.Map[String, String])] = rdd.map(row => {
//    val id1 = row.getString(0)
//    val year1 = row.getString(1)
//    val month1 = row.getLong(2)
//    val test1 = row.getMap[String, String](3)
//    (id1, year1, month1, test1)
//  })
//  rdd2.foreach(println(_))
  //frame2.show()
  session.stop()
}
