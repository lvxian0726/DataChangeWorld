package com.lvxian.feature

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.google.common.hash.Hashing
import com.vivo.ai.data.util.IOUtils
import com.vivo.ai.encode.ContinuousEncoder
import com.vivo.ai.encode.util.EncodeEnv
import com.vivo.ai.tuple
import com.vivo.vector.Vector

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * @program: feednews_smart_launcher_data_flow
  * @description: ${description}
  * @author: jkgong
  * @create: 2020-06-11 19:28
  */
object Tools {


  def String2CodingMap(featureValueStr: String, featureName: String = null): Map[Int, Double] = {
    if (!featureValueStr.equals(" ") && !featureValueStr.equals("") && featureValueStr != null) {
      featureValueStr.split(",")
        .map(value => {
          val kv: Array[String] = value.split(":")
          if (featureName == "top_category_v3_update" || featureName == "sub_category_v3_update" || featureName == "third_category_v3_update") {
            (ContinuousEncoder.encode(featureName, kv(0), EncodeEnv.PRD), kv(1).toDouble)
          } else if (featureName == "user_long_contenttag_v2" || featureName == "user_long_entity_v2") {
            (Math.abs(kv(0).hashCode) % 1000000, kv(1).toDouble)
          } else {
            (kv(0).toInt, kv(1).toDouble)
          }
        }).toMap
    } else {
      Map[Int, Double]()
    }
  }


  def getDurationSequence(tuple2Sequence: com.vivo.ai.tuple.Tuple2[Array[Long], Array[String]], serTimestamp: Long, intervals: Array[Long]): Array[Double] = {
    if (tuple2Sequence == null) {
      return new Array[Double](intervals.length)
    } else {
      val seqenceTimes = new Array[Double](intervals.length)
      val times = tuple2Sequence.getT1
      val values = tuple2Sequence.getT2.map(x =>
        if (x.contains("{")) {
          JSON.parseObject(x).getString("duration").toDouble
        } else {
          x.toDouble
        }
      )
      for (i <- 0 until times.length) {
        if (times(i) < serTimestamp) {
          for (j <- 0 until seqenceTimes.length) {
            if (times(i) > serTimestamp - intervals(j)) {
              seqenceTimes(j) += values(i).toDouble
            }
          }
        }
      }
      return seqenceTimes
    }
  }

  //由于top5类特征存在补0的情况因此需要过滤掉0
  def getHistoryVector(sequenceFeature: Array[Int], vectorSize: Int, filterValue: Int = Int.MaxValue): Vector = {
    if (sequenceFeature == null) {
      Vector.builder(vectorSize).index(Array(0)).value(Array(0.0F)).build()
    } else {
      var map = Map[Int, Double]()
      val values = sequenceFeature.filter(_ != filterValue)
      if (values.isEmpty) {
        Vector.builder(vectorSize).index(Array(0)).value(Array(0.0F)).build()
      } else {
        for (value <- values) {
          map += (value -> (map.getOrElse(value, 0.0) + 1.0))
        }
        map2Vec(map, vectorSize)
      }
    }
  }


  //由于top5类特征存在补0的情况因此需要过滤掉0
  def getDurationHistoryVector(sequenceFeature: tuple.Tuple3[Array[Long], Array[String], Array[Double]], vectorSize: Int, filterValue: Int = Int.MaxValue): Vector = {
    if (sequenceFeature == null) {
      Vector.builder(vectorSize).index(Array(0)).value(Array(0.0F)).build()
    } else {
      var map = Map[Int, Double]()
      val values: Array[(Int, Double)] = sequenceFeature.getT2.zip(sequenceFeature.getT3).map(x => (x._1.split("#"), x._2)).flatMap(x => x._1.map(y => (y.toInt, x._2))).filter(_ != filterValue)

      if (values.isEmpty) {
        Vector.builder(vectorSize).index(Array(0)).value(Array(0.0F)).build()
      } else {
        map2Vec(values.groupBy(x => x._1).map(x => (x._1, x._2.map(_._2).sum)), vectorSize)
      }
    }
  }

  def map2Vec(inputMap: scala.collection.immutable.Map[Int, Double], length: Int): com.vivo.vector.Vector = {
    if (inputMap.isEmpty) {
      Vector.builder(length).index(Array(0)).value(Array(0.0F)).build()
    } else {
      val sortedInputMap = inputMap.toArray.sortBy(_._1).filter(x => x._1 < length && x._2 > 0.0)
      if (sortedInputMap.size > 0) {
        val vec = Vector.builder(length)
          .index(sortedInputMap.map(_._1))
          .value(sortedInputMap.map(_._2.toFloat)).build()
        if (vec.toSparse.getIndices.length == 0) {
          Vector.builder(length).index(Array(0)).value(Array(0.0F)).build()
        } else {
          vec.toSparse
        }
      } else {
        Vector.builder(length).index(Array(0)).value(Array(0.0F)).build()
      }
    }
  }


  //json在如hbase后会擦除原始的类型，因此核对代码和正式入库是这个函数要做区分
  def getTuple2(json: JSONObject, key: String) = {
    val tuple2AndJsonArray = json.getObject(key, classOf[com.vivo.ai.tuple.Tuple2[JSONArray, JSONArray]])
    new com.vivo.ai.tuple.Tuple2(tuple2AndJsonArray.getT1.toArray.map(_.toString.toLong), tuple2AndJsonArray.getT2.toArray.map(_.toString))
  }

  //json在如hbase后会擦除原始的类型，因此核对代码和正式入库是这个函数要做区分
  def jsonGetMap(jsonObj: JSONObject, key: String) = {
    val jsonMap = jsonObj.getJSONObject(key)
    if (jsonMap != null) {
      jsonMap.toMap.map { x => (x._1.toInt, x._2.toString.toDouble) }
    } else {
      Map(0 -> 0.0)
    }
  }

  def parseFCJsonFeature(keyName: String, featureName: String, userJSONObject: JSONObject, dataSchema: String, takeNum: Int = 1, defaultFillValue: Int = 0): tuple.Tuple2[Array[Long], Array[String]] = {

    val timeStampBuffer = new ArrayBuffer[Long]()
    val featureBuffer = new ArrayBuffer[String]()

    var result: tuple.Tuple2[Array[Long], Array[String]] = null

    val concatList: List[Int] = Array.fill[Int](takeNum)(defaultFillValue).toList
    val jsons: Array[String] = userJSONObject.getObject(keyName, classOf[Array[String]])

    if (featureName != null && featureName != "" && jsons != null) {

      for (json <- jsons) {
        if (json != "" && json != null) {
          val jsonObject: JSONObject = JSON.parseObject(json)
          if (jsonObject == null) {

          } else {


            val event_time: String = jsonObject.getString("event_time")
            timeStampBuffer.add(event_time.toLong)

            var value: String = null
            if (dataSchema.toLowerCase == "string") {
              value = jsonObject.getString(featureName)

            } else if (dataSchema.toLowerCase == "json") {
              val innerJson: JSONObject = jsonObject.getJSONObject(featureName)

              if (featureName == "news_original_title_tag_v2" || featureName == "news_content_tag_vec_v2") { // 对这几个tag明码特征采用hash100w
                val features: List[Int] = innerJson.toMap.toList
                  .map(data => {
                    (Math.abs(data._1.hashCode % 1000000), data._2.toString.toDouble)
                  })
                  .sortBy(-_._2).map(_._1) ++ concatList
                value = features.map(_.toString).slice(0, takeNum).mkString("#")
              } else if (featureName.contains("topic")) { // 新序列特征中包含topic关键字的都是已经编过码的数据，可以直接用

                val features: List[Int] = innerJson.toMap.toList
                  .map(data => {
                    (data._1.toInt, data._2.toString.toDouble)
                  }).sortBy(-_._2).map(_._1) ++ concatList
                value = features.map(_.toString).slice(0, takeNum).mkString("#")
              } else { // 其他category类型的明码特征(v3 and v3update) 采用encoding映射文件中的值

                val features: List[Int] = innerJson.toMap.toList
                  .map(data => {
                    (ContinuousEncoder.encode(featureName, data._1, EncodeEnv.PRD), data._2.toString.toDouble)
                  })
                  .sortBy(-_._2).map(_._1) ++ concatList
                value = features.map(_.toString).slice(0, takeNum).mkString("#")
              }
            }
            featureBuffer.add(value)
          }
        }
      }


      if (timeStampBuffer.size == featureBuffer.size) {
        result = new com.vivo.ai.tuple.Tuple2(timeStampBuffer.toArray, featureBuffer.toArray)
        result
      } else {
        result = new com.vivo.ai.tuple.Tuple2(Array.empty[Long], Array.empty[String])
        result
      }
    } else {
      result = new com.vivo.ai.tuple.Tuple2(Array.empty[Long], Array.empty[String])
      result
    }
  }


  def parseFCJsonDurationFeature(keyName: String, featureName: String,
                                 userJSONObject: JSONObject,
                                 dataSchema: String, takeNum: Int = 1, defaultFillValue: Int = 0): tuple.Tuple3[Array[Long], Array[String], Array[Double]] = {

    val timeStampBuffer = new ArrayBuffer[Long]()
    val featureBuffer = new ArrayBuffer[String]()
    val durationFeatureBuffer = new ArrayBuffer[Double]()

    var result: tuple.Tuple3[Array[Long], Array[String], Array[Double]] = null

    val concatList: List[Int] = Array.fill[Int](takeNum)(defaultFillValue).toList
    val jsons: Array[String] = userJSONObject.getObject(keyName, classOf[Array[String]])

    if (featureName != null && featureName != "" && jsons != null) {

      for (json <- jsons) {
        if (json != "" && json != null) {
          val jsonObject: JSONObject = JSON.parseObject(json)
          if (jsonObject == null) {

          } else {
            val event_time: String = jsonObject.getString("event_time")
            val duration: Double = try {
              jsonObject.getString("read_duration").toDouble
            } catch {
              case e: Throwable => 0.0
            }
            timeStampBuffer.add(event_time.toLong)
            durationFeatureBuffer.add(duration)


            var value: String = null
            if (dataSchema.toLowerCase == "string") {
              value = jsonObject.getString(featureName)

            } else if (dataSchema.toLowerCase == "json") {
              val innerJson: JSONObject = jsonObject.getJSONObject(featureName)

              if (featureName == "news_original_title_tag_v2" || featureName == "news_content_tag_vec_v2") { // 对这几个tag明码特征采用hash100w
                val features: List[Int] = innerJson.toMap.toList
                  .map(data => {
                    (Math.abs(data._1.hashCode) % 1000000, data._2.toString.toDouble)
                  })
                  .sortBy(-_._2).map(_._1) ++ concatList
                value = features.map(_.toString).slice(0, takeNum).mkString("#")
              } else if (featureName.contains("topic")) { // 新序列特征中包含topic关键字的都是已经编过码的数据，可以直接用

                val features: List[Int] = innerJson.toMap.toList
                  .map(data => {
                    (data._1.toInt, data._2.toString.toDouble)
                  }).sortBy(-_._2).map(_._1) ++ concatList
                value = features.map(_.toString).slice(0, takeNum).mkString("#")
              } else { // 其他category类型的明码特征(v3 and v3update) 采用encoding映射文件中的值

                val features: List[Int] = innerJson.toMap.toList
                  .map(data => {
                    (ContinuousEncoder.encode(featureName, data._1, EncodeEnv.PRD), data._2.toString.toDouble)
                  })
                  .sortBy(-_._2).map(_._1) ++ concatList
                value = features.map(_.toString).slice(0, takeNum).mkString("#")
              }
            }
            featureBuffer.add(value)
          }
        }
      }
      if (timeStampBuffer.size == featureBuffer.size && featureBuffer.size == durationFeatureBuffer.size) {
        result = new com.vivo.ai.tuple.Tuple3(timeStampBuffer.toArray, featureBuffer.toArray, durationFeatureBuffer.toArray)
        result
      } else {
        result = new com.vivo.ai.tuple.Tuple3(Array.empty[Long], Array.empty[String], Array.empty[Double])
        result
      }

    } else {
      result = new com.vivo.ai.tuple.Tuple3(Array.empty[Long], Array.empty[String], Array.empty[Double])
      result
    }
  }


  def jsonGetStringMap(jsonObj: JSONObject, key: String) = {

    try {
      val jsonMap = jsonObj.getJSONObject(key)
      if (jsonMap != null) {
        jsonMap.toMap.map {
          x => (x._1.toString, x._2.toString.toDouble)
        }
      } else {
        Map("" -> 0.0)
      }
    } catch {
      case e => {
        println("解析出现异常：")
        println(jsonObj.toString())
        e.printStackTrace()
        Map("" -> 0.0)
      }
    }


  }

  def vec2TFSparseVec(vec: Vector, defalutValue: Vector = null): (Array[Int], Array[Double]) = {
    val (vecIndices, vecValues) = if (vec == null) {
      if (defalutValue != null) {
        (defalutValue.getIndices, defalutValue.getValues)
      } else {
        (Array(0), Array(0.0F))
      }
    } else {
      if (vec.numNonzeros() == 0) {
        if (defalutValue != null) {
          (defalutValue.getIndices, defalutValue.getValues)
        }
        else {
          (Array(0), Array(0.0F))
        }
      }
      //values中间有0.0值需要过滤掉
      else if (vec.numNonzeros() < vec.getValues.length) {
        val tmp = vec.getIndices.zip(vec.getValues).filter(_._2 != 0.0)
        (tmp.map(_._1), tmp.map(_._2))
      }
      else {
        (vec.getIndices, vec.getValues)
      }
    }

    if (vecIndices.length == vecValues.length) {
      (vecIndices, vecValues.map(_.toDouble))
    } else {
      (Array(0), Array(0.0D))
    }
  }


  def jsonGetOrElseDouble(jsonObj: JSONObject, key: String, default: Double = 0.0): Double = {
    if (jsonObj.containsKey(key)) {
      if (jsonObj.getString(key) == """{"compress":false}""" || jsonObj.getDoubleValue(key).isNaN || jsonObj.getDoubleValue(key).isInfinity) {
        default
      } else {
        jsonObj.getDoubleValue(key)
      }
    } else {
      default
    }
  }

  def getExperimentID(imei: String, salt: String = null, range: Int): Int = {
    if (salt == null) {
      Math.abs(Hashing.murmur3_32.hashBytes(imei.getBytes("UTF-8")).asInt()) % range
    } else {
      Math.abs(Hashing.murmur3_32.hashBytes((imei + salt).getBytes("UTF-8")).asInt()) % range
    }
  }

  def findClickInShow(show: Array[String], click: Array[String]): Array[Int] = {
    val uniqueClickNewsid: Set[String] = click.toSet
    val flag: Array[Int] = show.map {
      case newsid => {
        var actionFlag: Int = 0
        if (uniqueClickNewsid.contains(newsid)) {
          actionFlag = 1
        }
        actionFlag
      }
    }
    flag
  }

}



