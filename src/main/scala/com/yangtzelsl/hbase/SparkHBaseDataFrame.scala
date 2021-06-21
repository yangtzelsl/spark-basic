package com.yangtzelsl.hbase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by luis.liu
  *
  * 参考文档：https://blog.csdn.net/yitengtongweishi/article/details/82556824
  */

object SparkHBaseDataFrame {
  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("SparkHBaseDataFrame").getOrCreate()

    val url = s"jdbc:phoenix:localhost:2181"
    val dbtable = "PHOENIXTEST"

    //spark 读取 phoenix 返回 DataFrame 的 第一种方式
    val rdf = spark.read
      .format("jdbc")
      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      .option("url", url)
      .option("dbtable", dbtable)
      .load()
    rdf.printSchema()

    //spark 读取 phoenix 返回 DataFrame 的 第二种方式
    val df = spark.read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> dbtable, "zkUrl" -> url))
      .load()
    df.printSchema()

    //spark DataFrame 写入 phoenix，需要先建好表
    df.write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .options(Map("table" -> "PHOENIXTESTCOPY", "zkUrl" -> url))
      .save()

    spark.stop()
  }
}
