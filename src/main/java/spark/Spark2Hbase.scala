package spark

import org.apache.hadoop.hbase.{HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import util.HbaseUtil
import java.util

import org.apache.hadoop.hbase.filter._


object Spark2Hbase extends App {
  private val conn: Connection = HbaseUtil.getHBaseConn
  private val tableName: TableName = TableName.valueOf("cdc_table")
  private val table: Table = conn.getTable(tableName)

  def insert_row(table: Table, row_key: Array[Byte]) = {
    val put = new Put(row_key)
    val family = Bytes.toBytes("info")
    val column_name = Bytes.toBytes("name")
    val column_sex = Bytes.toBytes("sex")
    val column_age = Bytes.toBytes("age")
    val value_name = Bytes.toBytes("lvxian")
    val sex_value = Bytes.toBytes("man")
    val age_value = Bytes.toBytes("26")
    put.addColumn(family, column_name, value_name)
    put.addColumn(family, column_sex, sex_value)
    put.addColumn(family, column_age, age_value)
    table.put(put)
  }


  def deleteByRowKeyAndColumn(table: Table, row_key: Array[Byte], family: Array[Byte], column: Array[Byte]) = {
    val delete = new Delete(row_key)
    delete.addColumn(family, column)
    table.delete(delete)
  }


  def checkAndDelete(table: Table, row_key: Array[Byte], family: Array[Byte], column: Array[Byte], value: Array[Byte]) = {
    val tmp_rowkey = Bytes.toBytes("1586162948353")
    val tmp_family = Bytes.toBytes("info")
    val tmp_column = Bytes.toBytes("param1")
    val tmp_vlaue = Bytes.toBytes("lvxian")

    val delete = new Delete(row_key)
    delete.addColumn(family, column)

    try {
      val delete_status = table.checkAndDelete(tmp_rowkey, tmp_family, tmp_column, null, delete)
      println("返回状态:" + delete_status.toString)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  val family = Bytes.toBytes("info")
  val column = Bytes.toBytes("age")
  val value = Bytes.toBytes("lvxian")
  val row_key: Array[Byte] = Bytes.toBytes("1586166799127")
  val start_rowkey: Array[Byte] = Bytes.toBytes("1586166799127")
  val end_rowkey: Array[Byte] = Bytes.toBytes("1586166799127")


  //  checkAndDelete(table = table, row_key = row_key, family = family, column = column, value = value)
  //
  //  insert_row(table, row_key = row_key)
  //
  //  deleteByRowKeyAndColumn(table, row_key = row_key, family = family, column = column)

  def getDataByScanner(table: Table, start_rowkey: Array[Byte], end_rowkey: Array[Byte], family: Array[Byte], column: Array[Byte]) = {

    val scan = new Scan()
    scan.setStartRow(start_rowkey)
    scan.setStopRow(end_rowkey)
    scan.addColumn(family, column)

    val scanner: ResultScanner = table.getScanner(scan)

    val value: util.Iterator[Result] = scanner.iterator()

    while(value.hasNext){
      val next: Result = value.next()
      val only_value = Bytes.toString(next.getValue(family,column))
      println(next)
      println(only_value)
    }
    scanner.close()

  }

  def rowFilterScan(table: Table) = {
    val scan = new Scan()

    val filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("1586166799127")))

    scan.setFilter(filter1)

    val resultScanner = table.getScanner(scan)

    val value: util.Iterator[Result] = resultScanner.iterator()

    while(value.hasNext){
      val next: Result = value.next()
      println(next)
    }
    resultScanner.close()

    println("*"*100)
    val filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*7$"))

    scan.setFilter(filter2)

    val resultScanner2 = table.getScanner(scan)

    val value2: util.Iterator[Result] = resultScanner2.iterator()

    while(value2.hasNext){
      val next: Result = value2.next()
      println(next)
    }
    resultScanner2.close()

    println("*"*100)

    val filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("16679912"))

    scan.setFilter(filter3)

    val resultScanner3 = table.getScanner(scan)

    val value3: util.Iterator[Result] = resultScanner3.iterator()

    while(value3.hasNext){
      val next: Result = value3.next()
      println(next)
    }
    resultScanner3.close()

  }

  rowFilterScan(table = table)

//  getDataByScanner(table = table, start_rowkey = row_key, end_rowkey = row_key, family = family, column = column)



}
