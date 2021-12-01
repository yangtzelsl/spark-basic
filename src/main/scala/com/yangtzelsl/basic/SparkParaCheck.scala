package com.yangtzelsl.basic

/**
 * 写参数校验代码
 */
object SparkParaCheck {

  def main(args: Array[String]): Unit = {
    // 检验参数
    if (args.length != 3) {
      println(
        """
          |Usage: data locker load data from s3
          |Param:
          | dataType：pba数据的类型 cohort_retargeting | cohort_unified | cohort_user_acquisition
          |           conversion_paths |
          |           skad_inapps | skad_installs | skad_postbacks_copy | skad_postbacks | skad_redownloads |
          |           website_assisted_installs | website_events | website_visits
          | date：数据的日期，2021-11-24
          | env：环境 ppe_xxx | xxx
          """.stripMargin)
      // -1 非正常退出
      sys.exit(-1)
    }
  }

}
