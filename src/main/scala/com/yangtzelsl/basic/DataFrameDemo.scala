package com.yangtzelsl.basic

import org.apache.spark.sql.SparkSession

/**
 *
 * @Description: DataFrameDemo
 * @Author luis.liu
 * @Date: 2021/6/29 10:40
 * @Version 1.0
 */
object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    // 初始化SparkSession
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._
    // 方式1.1 本地seq + toDF创建DataFrame
    // 直接用toDF()而不指定列名字，那么默认列名为"_1", "_2", ...
    val df1 = Seq(
      (1, "First Value", java.sql.Date.valueOf("2010-01-01")),
      (2, "Second Value", java.sql.Date.valueOf("2010-02-01"))
    ).toDF("int_column", "string_column", "date_column")
    df1.show()
    df1.printSchema()

    // 方式1.2 case class + toDF创建DataFrame

    // 方法二，Spark中使用createDataFrame函数创建DataFrame

    // 方法三，通过文件直接创建DataFrame
  }
}
