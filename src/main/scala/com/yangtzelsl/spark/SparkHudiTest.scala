package com.yangtzelsl.spark

import com.yangtzelsl.conf.ConfigurationManagerJava
import com.yangtzelsl.constant.ConstantsConfigJava
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.reflect.io.Directory

case class Album(albumId: Long, title: String, tracks: Array[String], updateDate: Long)

case class AlbumInc(albumId: Long, title: String, tracks: Array[String], Inc: String, updateDate: Long, Inc1: String)

case class AlbumInc2(albumId: Long, title: String, tracks: Array[String], Inc: String, updateDate: Long, Inc1: String, Inc2: String)

case class AlbumInc3(albumId: Long, title: String, tracks: Array[String], Inc: String, updateDate: Long, Inc1: String, Inc2: String, Inc3: String)

case class AlbumInc4(albumId: Long, title: String, tracks: Array[String], Inc: String, updateDate: Long, Inc1: String, Inc2: String, Inc3: String, Inc4: String)

/**
 *
 * @Description: SparkHudiTest
 * @Author luis.liu
 * @Date: 2021/5/30 12:59
 * @Version 1.0
 */
object SparkHudiTest {

  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val INITIAL_ALBUM_DATA = Seq(
    Album(800, "6 String Theory", Array("Lay it down", "Am I Wrong", "68"), dateToLong("2019-12-01")),
    Album(800, "6 String Theory jj", Array("Lay it down", "Am I Wrong", "69"), dateToLong("2019-12-01")),
    Album(801, "Hail to the Thief", Array("2+2=5", "Backdrifts"), dateToLong("2019-12-01")),
    Album(801, "Hail to the Thief", Array("2+2=5", "Backdrifts", "Go to sleep"), dateToLong("2019-12-03"))
  )
  private val UPSERT_ALBUM_DATA = Seq(
    AlbumInc(800, "6 String Theory", Array("Jumpin' the blues", "Bluesnote", "Birth of blues"), "", dateToLong("2019-12-01"), null),
    AlbumInc(801, "Hail to the Thief", Array("2+2=5 ssss", "Backdrifts"), "ll", dateToLong("2019-12-01"), "hahahahah")
  )

  private val UPSERT_ALBUM_DATA2 = Seq(
    AlbumInc2(801, "Hail to the Thief", Array("2+2=5 ssss", "Backdrifts"), "ll", dateToLong("2019-12-01"), "hahahahah", "aaaaaaaaa")
  )

  private val UPSERT_ALBUM_DATA3 = Seq(
    AlbumInc3(801, "Hail to the Thief", Array("2+2=5 ssss", "Backdrifts"), "ll", dateToLong("2019-12-01"), "hahahahah", "aaaaaaaaa", "3333333----")
  )

  private val UPSERT_ALBUM_DATA4 = Seq(
    AlbumInc4(801, "Hail to the Thief", Array("2+2=5 ssss", "Backdrifts"), "ll", dateToLong("2019-12-01"), "hahahahah", "aaaaaaaaa", "3333333", "444444")
  )

  def dateToLong(dateString: String): Long = LocalDate.parse(dateString, formatter).toEpochDay

  def main(args: Array[String]) {

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

    // 获取配置文件名
    val configuration = ConfigurationManagerJava.getPropConfig(args(0))

    // 从配置文件中获取路径
    val basePath = configuration.getString(ConstantsConfigJava.S3_HUDI_TABLE_BASIC_PATH)

    // clearDirectory()
    // System.setProperty("HADOOP_USER_NAME", "hive")

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val tableName = "Album"
    upsert(INITIAL_ALBUM_DATA.toDF(), tableName, "albumId", "updateDate", basePath)
    snapshotQuery(spark, tableName, basePath)


    upsert(UPSERT_ALBUM_DATA.toDF(), tableName, "albumId", "updateDate", basePath)

  }

  /**
   * 更新查询
   *
   * @param albumDf
   * @param tableName
   * @param key
   * @param combineKey
   */
  private def upsert(albumDf: DataFrame, tableName: String, key: String, combineKey: String, basePath: String): Unit = {
    albumDf.write
      .format("org.apache.hudi")
      // 默认模式，更新并插入
      // 属性：hoodie.datasource.write.operation, 默认值：upsert
      // 是否为写操作进行插入更新、插入或批量插入。使用bulkinsert将新数据加载到表中，之后使用upsert或insert。 批量插入使用基于磁盘的写入路径来扩展以加载大量输入，而无需对其进行缓存。
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      // 写时复制
      //      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      // 读时合并
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // 属性：hoodie.datasource.write.recordkey.field, 默认值：uuid
      // 记录键字段。用作HoodieKey中recordKey部分的值。 实际值将通过在字段值上调用.toString()来获得。可以使用点符号指定嵌套字段，例如：a.b.c
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "albumId, title")
      // 属性：hoodie.datasource.write.precombine.field, 默认值：ts
      // 实际写入之前在preCombining中使用的字段。 当两个记录具有相同的键值时，我们将使用Object.compareTo(..)从precombine字段中选择一个值最大的记录。
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, combineKey)
      // 属性：hoodie.datasource.write.partitionpath.field, 默认值：partitionpath
      // 分区路径字段。用作HoodieKey中partitionPath部分的值。 通过调用.toString()获得实际的值
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "updateDate")
      // 属性：hoodie.datasource.write.keygenerator.class, 默认值：org.apache.hudi.SimpleKeyGenerator
      // 键生成器类，实现从输入的Row对象中提取键
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, "org.apache.hudi.keygen.ComplexKeyGenerator")
      .option(HoodieWriteConfig.TABLE_NAME, tableName)


      // 属性：hoodie.insert.shuffle.parallelism, hoodie.upsert.shuffle.parallelism
      // 最初导入数据后，此并行度将控制用于读取输入记录的初始并行度。 确保此值足够高，例如：1个分区用于1 GB的输入数据
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")

      // 以下参数针对 hive 同步
      // 属性：hoodie.datasource.hive_sync.jdbcurl, 默认值：jdbc:hive2://localhost:10000
      // Hive metastore url
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://x.x.x.x:10000")
      // 属性：hoodie.datasource.hive_sync.enable, 默认值：false
      // 设置为true时，将数据集注册并同步到Apache Hive Metastore
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      // 属性：hoodie.datasource.hive_sync.database, 默认值：default
      // 要同步到的数据库
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "your_database")
      // 属性：hoodie.datasource.hive_sync.table, [Required]
      // 要同步到的表
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, tableName)
      // 属性：hoodie.datasource.hive_sync.partition_fields, 默认值：
      // 数据集中用于确定Hive分区的字段。
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "updateDate")
      // 属性：hoodie.datasource.hive_sync.partition_extractor_class, 默认值：org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
      // 用于将分区字段值提取到Hive分区列中的类。
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName)

      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, "true")

      // 指定 compaction参数
      .option("hoodie.compact.inline", "true")
      .option("hoodie.compact.inline.max.delta.commits", "10")

      .mode(SaveMode.Append)
      .save(s"$basePath/$tableName/")
  }

  /**
   * 清空目录
   */
  private def clearDirectory(basePath: String): Unit = {
    val directory = new Directory(new File(basePath))
    directory.deleteRecursively()
  }

  /**
   * 快照查询
   *
   * @param spark
   * @param tableName
   */
  private def snapshotQuery(spark: SparkSession, tableName: String, basePath: String): Unit = {
    spark.read.format("hudi").load(s"$basePath/$tableName/*").show()
  }

  /**
   * 增量查询
   *
   * @param spark
   * @param basePath
   * @param tableName
   */
  private def incrementalQuery(spark: SparkSession, basePath: String, tableName: String): Unit = {
    spark.read
      .format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20200412183510")
      .load(s"$basePath/$tableName")
      .show()

  }

  /**
   * 删除查询
   *
   * @param spark
   * @param basePath
   * @param tableName
   */
  private def deleteQuery(spark: SparkSession, basePath: String, tableName: String): Unit = {
    val deleteKeys = Seq(
      Album(803, "", null, 0l),
      Album(802, "", null, 0l)
    )

    import spark.implicits._

    val df = deleteKeys.toDF()

    df.write.format("hudi")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "albumId")
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .mode(SaveMode.Append) // Only Append Mode is supported for Delete.
      .save(s"$basePath/$tableName/")

    spark.read.format("hudi").load(s"$basePath/$tableName/*").show()
  }
}

