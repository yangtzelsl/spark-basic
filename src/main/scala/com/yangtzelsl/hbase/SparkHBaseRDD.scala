package com.yangtzelsl.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
//import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by luis.liu.
  */

object SparkHBaseRDD {
  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("SparkHBaseRDD").getOrCreate()
    val sc = spark.sparkContext

    val tablename = "SparkHBase"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","localhost")  //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")       //设置zookeeper连接端口，默认2181
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    // 初始化job，TableOutputFormat 是 org.apache.hadoop.hbase.mapred 包下的
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val indataRDD = sc.makeRDD(Array("2,jack,16", "1,Lucy,15", "5,mike,17", "3,Lily,14"))

    val rdd = indataRDD.map(_.split(',')).map{ arr=>
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据 须用 org.apache.hadoop.hbase.util.Bytes.toBytes 转换
       * Put.addColumn 方法接收三个参数：列族，列名，数据*/
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes(arr(2)))
      (new ImmutableBytesWritable, put)
    }
    rdd.saveAsHadoopDataset(jobConf)

    spark.stop()
  }
}
