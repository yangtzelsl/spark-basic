package com.yangtzelsl.basic

import org.apache.spark.sql.SparkSession

/**
 *
 * @Description:
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
      .json("file:///D:\\IDEA2020\\spark-basic\\src\\main\\resources\\data.json")
      .toDF()

    df.show()

    session.stop()
  }
}
