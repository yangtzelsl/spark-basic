package com.yangtzelsl.spark

import com.yangtzelsl.conf.ConfigurationManagerJava
import com.yangtzelsl.constant.ConstantsConfigJava
import org.apache.spark.sql.SparkSession

/**
 *
 * @Description: SparkInputDemo
 * @Author luis.liu
 * @Date: 2021/6/18 17:34
 * @Version 1.0
 */
object SparkInputDemo {
  def main(args: Array[String]): Unit = {
    // 检验参数
    if (args.length != 1) {
      println(
        """
          |Usage: spark-submit xxx.jar config-test.properties
          |Param:
          | configFile: 配置文件(config-test.properties 或者 config-prod.properties)
          |Info:
          | 请传递对应的配置文件参数!
        """.stripMargin)
      sys.exit(-1) // -1 非正常退出
    }

    // 获取配置项
    val configuration = ConfigurationManagerJava.getPropConfig(args(0))

    val inputFile = configuration.getString(ConstantsConfigJava.INPUT_FILE)

    // 初始化SparkSession
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    // 加载数据
    val jsonDF = spark
      .read
      // json多行写才不会报错，否则一条json数据只能一行一行写
      .option("multiline", "true")
      // 加载本地文件
      .json(inputFile)
      .toDF()

    // transform
    jsonDF.show()
    jsonDF.printSchema()

    jsonDF.createOrReplaceTempView("test")
    spark.sql(
      """
        | select
        |  *
        | from
        |  test
        |
        |""".stripMargin)
      .show()

    // 关闭资源
    spark.stop()
  }
}
