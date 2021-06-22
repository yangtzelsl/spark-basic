package com.yangtzelsl.hbase

import com.yangtzelsl.utils.HBaseUtils
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 *
 * @Description: SparkHBasePut Spark写HBase方法1
 * @Author luis.liu
 * @Date: 2021/6/22 16:17
 * @Version 1.0
 */
object SparkHBasePut {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseWriteTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val tableName = "imooc_course_clickcount"
    val quorum = "localhost"
    val port = "2181"

    // 配置相关信息

    val conf = HBaseUtils.getHBaseConfiguration(quorum,port,tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)


    val indataRDD = sc.makeRDD(Array("002,10","003,10","004,50"))

    indataRDD.foreachPartition(x=> {
      val conf = HBaseUtils.getHBaseConfiguration(quorum,port,tableName)
      conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

      val htable = HBaseUtils.getTable(conf,tableName)

      x.foreach(y => {
        val arr = y.split(",")
        val key = arr(0)
        val value = arr(1)

        val put = new Put(Bytes.toBytes(key))
        put.add(Bytes.toBytes("info"),Bytes.toBytes("clict_count"),Bytes.toBytes(value))
        htable.put(put)
      })
    })

    sc.stop()

  }
}
