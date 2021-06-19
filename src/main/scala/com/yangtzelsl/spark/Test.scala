package com.yangtzelsl.spark

import org.apache.spark.sql.SparkSession

/**
 *
 * @Description: Test
 * @Author luis.liu
 * @Date: 2021/6/18 19:42
 * @Version 1.0
 */
object Test {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val df = session
      .read
      .option("multiline", "true")
      .json("file:///data.json")
      .toDF()

    df.show()

    session.stop()
  }
}
