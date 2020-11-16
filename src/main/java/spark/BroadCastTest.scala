package spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadCastTest {

  val args_1 = 2

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")

    val sc = new SparkContext(sparkConf)


    val list_cn = List("中国", "美国", "德国")

    val author: List[String] = List("中国晚报", "美国之音", "的国企成", "德国汽车")

    val author_rdd: RDD[String] = sc.makeRDD(author)

    val br: Broadcast[List[String]] = sc.broadcast[List[String]](list_cn)

    val data: RDD[String] = author_rdd.repartition(4).map(value => {
      //    val get_broadcast_value = br.value
      //    println(value)
      //    println("____________________")
      //    get_broadcast_value.foreach(println(_))
      println("local args:" + args_1)
      value
    })
    val result: Array[String] = data.collect()

    result.foreach(println(_))

    sc.stop()
  }

}
