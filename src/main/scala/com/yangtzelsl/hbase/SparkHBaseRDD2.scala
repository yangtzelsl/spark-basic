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

object SparkHBaseRDD2 {
  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("SparkHBaseRDD").getOrCreate()
    val sc = spark.sparkContext

    val tablename = "SparkHBase"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","10.132.58.88")  //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")       //设置zookeeper连接端口，默认2181
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tablename)

    // 如果表不存在，则创建表
    val admin = new HBaseAdmin(hbaseConf)
    if (!admin.isTableAvailable(tablename)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
      admin.createTable(tableDesc)
    }

    //读取数据并转化成rdd TableInputFormat 是 org.apache.hadoop.hbase.mapreduce 包下的
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hBaseRDD.foreach{ case (_ ,result) =>
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("cf1".getBytes,"name".getBytes))
      val age = Bytes.toString(result.getValue("cf1".getBytes,"age".getBytes))
      println("Row key:"+key+"\tcf1.Name:"+name+"\tcf1.Age:"+age)
    }
    admin.close()

    spark.stop()
  }
}
