package com.yangtzelsl.spark

import com.yangtzelsl.conf.ConfigurationManagerJava
import com.yangtzelsl.constant.ConstantsConfigJava
import org.apache.spark.rdd.RDD
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
      // 加载本地文件，使用相对路径，通用写法
      .json(this.getClass.getResource("/").toString + inputFile)
      .toDF()

    // file:/D:/IDEA2020/spark-basic/target/classes/
    println(this.getClass.getResource("/").toString)

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

  def testJson(spark: SparkSession, inputFile: String): Unit = {

    val sc = spark.sparkContext

    val jsonStrRDD = sc.textFile(inputFile)

    import spark.implicits._
    val mapRDD: RDD[String] = jsonStrRDD.map(
      t => t
        .replaceAll("\"$", "\"")
        .toLowerCase
    )
    // 收集到driver端打印
    mapRDD.collect().foreach(println)

    val ds = mapRDD.toDS()
    ds.show()
    ds.createOrReplaceTempView("t1")

    // 测试数据大小写问题
    /**
     * 结论：
     * 1.查询和数据原本大小写保持一致，即数据是大写，JSON获取用大写
     * 2.全部转小写，JSON数据获取也用小写(推荐采用该方案)
     */
    val ds_upper = spark.sql(
      """
        | select
        | cast(get_json_object(value,"$.properties.kyc_pre_status") as int)	kyc_pre_status,
        | cast(get_json_object(value,"$.properties.kyc_pre_status1") as int)	kyc_pre_status1,
        | cast(get_json_object(value,"$.properties.KYC_status") as int)	KYC_status
        | from t1
        |""".stripMargin)
      .toDF()
      .show()
  }
}
