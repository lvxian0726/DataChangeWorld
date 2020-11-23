package com.lvxian.utils

import java.io.InputStream

import scala.io.{BufferedSource, Source}

object IOutil {

  /**
    * 用于读取resources 文件中的数据, "#" 开头的行会被过滤
    * @param fileName resources 文件中的相对路径
    * @return
    */
  def fromResource(fileName:String):Seq[String] = {
    var in: InputStream = null
    try{
      in = getClass.getClassLoader.getResourceAsStream(fileName)
      val lines = Source.fromInputStream(in)
      lines.getLines().toArray.toSeq.filter(!_.startsWith("#"))
    }catch {
      case e:Exception =>
//        logger.error("read resource error ",e)
        Seq[String]()
    }finally {
      if(in != null){
        in.close()
      }
    }
  }

  /**
    * 用于读取操作系统中的文件, "#" 开头的行会被过滤
    * @param filePath 操作系统中的路径，可以是相对路径，也可以是绝对路径
    */
  def fromFile(filePath:String):Seq[String] = {
    var source:BufferedSource = null
    try{
      source = Source.fromFile(filePath)
      source.getLines().toArray.toSeq.filter(!_.startsWith("#"))
    }catch {
      case e:Exception =>
//        logger.error("read file error ",e)
        Seq[String]()
    }finally {
      if(source != null){
        source.close()
      }
    }
  }


  /**
    * /**
    * * 根据token 生成hbase rowkey, 相当于一个请求对应一个rowkey
    * * @param ttl          过期时间和hbase 表设置的时间一致，单位毫秒
    * * @param partitionNum hash partition 的数量
    * * @return rowkey 分桶id(3byte)+时间戳(10byte)+uuid(9byte)
    **/
    * def getKey(uuid: String, timestamp: Long, ttl: Long, partitionNum: Int = 1000): String = {
    * val key = new StringBuilder
    * // 时间戳后3位作为数据分桶id (3)
    *     key.append(getHashId(uuid, partitionNum))
    * // 时间戳变成ttl余数
    *     key.append((timestamp % ttl / 1000).formatted("%010d"))
    * // uuid取 (9)
    *     key.append(getSampledUUID(uuid, timestamp))
    *     key.toString
    * }
    */

}
