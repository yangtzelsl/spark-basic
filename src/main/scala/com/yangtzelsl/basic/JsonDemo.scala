package com.yangtzelsl.basic

import org.apache.spark.sql.SparkSession

/**
 * spark dataframe 读取 多行JSON
 */
object JsonDemo {

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .appName("sql")
      .master("local")
      .getOrCreate()

    val df = session
      .read
      .option("multiline", "true")
      .json("file:///D:\\IDEA2020\\spark-basic\\src\\main\\resources\\data.json")
      .toDF()

    df.show()

    session.stop()

  }

}
