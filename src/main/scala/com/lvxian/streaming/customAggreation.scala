package com.lvxian.streaming

import org.apache.flink.api.common.functions.AggregateFunction

object customAggreation extends AggregateFunction[(String, Int), (String, Int), (String, Int)] {
  override def createAccumulator(): (String, Int) = (null, 0)

  override def add(in: (String, Int), acc: (String, Int)): (String, Int) = {
    if (acc._1 == null) {
      (in._1, 1)
    } else {
      (acc._1, acc._2 + 1)
    }
  }

  override def getResult(acc: (String, Int)): (String, Int) = acc

  override def merge(acc: (String, Int), acc1: (String, Int)): (String, Int) = {
    (acc._1, acc._2 + acc1._2)
  }
}
