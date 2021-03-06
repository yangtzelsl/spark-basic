package com.yangtzelsl.hudi

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 测试情景：
 * 1. kafka -> spark streaming -> hive -> HuDi
 * 2. Spark SQL DataFrame 实现 HuDi 增量更新
 */
object AWSKafkaTutorialTopic {

  /**
   * Set nullable property of column. 设置指定列默认为空属性
   * @param df source DataFrame
   * @param cn is the column name to change
   * @param nullable is the flag to set, such that the column is  either nullable or not
   */
  def setNullableStateOfColumn( df: DataFrame, cn: String, nullable: Boolean) : DataFrame = {

    // get schema
    val schema: StructType = df.schema
    // modify [[StructField] with name `cn`
    val newSchema: StructType = StructType(schema.map {
      case StructField( c, t, _, m) if c.equals(cn) => StructField( c, t, nullable = nullable, m)
      case y: StructField => y
    })
    // apply new schema
    df.sqlContext.createDataFrame( df.rdd, newSchema )
  }

  /**
   * 设置所有列默认为空属性
   * @param df 源dataframe
   * @param nullable 为空属性
   * @return
   */
  def setNullableStateForAllColumns( df: DataFrame, nullable: Boolean) : DataFrame = {
    // get schema
    val schema: StructType = df.schema
    // modify [[StructField] with name `cn`
    val newSchema: StructType = StructType(schema.map {
      case StructField( c, t, _, m) ⇒ StructField( c, t, nullable = nullable, m)
    })
    // apply new schema
    df.sqlContext.createDataFrame( df.rdd, newSchema )
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .set("spark.driver.maxResultSize", "1g")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(20))

    val kafkaParam = Map(
      "bootstrap.servers" -> "your_broker_list",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "AWSKafkaTutorialTopic",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean),
      "security.protocol" -> "SSL",
      "ssl.truststore.location" -> "kafka.client.truststore.jks"
    )

    val topics = Array("AWSKafkaTutorialTopic")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )

    stream.foreachRDD(rdd => {
      import sparkSession.implicits._
      val rddd = rdd.map(item => {
        var v1: String = item.value()
        v1
      })

      val df = sparkSession.createDataset(rddd).toDF("word")

      df.createOrReplaceTempView("words")

      val sql1 = "select word id, '2015-01-01' creation_date, '2015-01-01T13:51:39.340396Z' last_update_time from words"
      val sql2 = "select word id, '2015-01-01' creation_date, '2015-01-01T13:51:39.340396Z' last_update_time, 'A' inc1, 'C' inc2, 'E' inc3 from words"
      /**
       * 着重说明：
       * 在spark数据类型中，
       *   如果新增的字段是integer等数字类型，默认的nullable=false，此时需要控制其默认属性nullable=false，才能保证HuDi在新增字段时，不报错
       *   如果新增的字段是string类型，默认的nullable=true，则没有该隐患
       */

      val inputDF = sparkSession.sql(sql2) // 切换此 SQL 测试新增字段

      inputDF
        .write
        .format("hudi")
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "creation_date")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "last_update_time")
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, "org.apache.hudi.keygen.ComplexKeyGenerator")
        .option(HoodieWriteConfig.TABLE_NAME, "hudi_test11")
        .option("hoodie.insert.shuffle.parallelism", "2")
        .option("hoodie.upsert.shuffle.parallelism", "2")
        .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://x.x.x.x:10000")
        .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
        .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default")
        .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "hudi_test11")
        .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "creation_date")
        .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName)
        .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, "true")
        .option("hoodie.compact.inline", "true")
        .option("hoodie.compact.inline.max.delta.commits", "10")
        .mode(SaveMode.Append)
        .save("s3://xxxx/data/tracking-data/test_hudi11/")
    })

    streamingContext.start()

    streamingContext.awaitTermination()
  }
}