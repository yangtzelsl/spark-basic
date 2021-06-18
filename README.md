# 说明
spark 基础代码

将参数配置化，便于快速调式，上线

# 任务提交

```shell
nohup spark-submit \
--master yarn  \
--deploy-mode cluster    \
--driver-memory 4G \
--num-executors 3   \
--executor-memory 2G  \
--executor-cores 3  \
--conf spark.default.parallelism=100  \
--conf spark.dynamicAllocation.enabled=true  \
--conf spark.dynamicAllocation.minExecutors=3  \
--conf spark.dynamicAllocation.maxExecutors=20  \
--conf spark.dynamicAllocation.initialExecutors=10  \
--conf spark.executor.memoryOverhead=1g  \
--conf spark.locality.wait=100ms \
--conf spark.shuffle.service.enabled=true  \
--class com.amber.sensorsdata.spark.SensorsEventTopic \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"  \
--conf "spark.sql.hive.convertMetastoreParquet=false"  \
--jars /xxx/*.jar,/usr/lib/spark/jars/*.jar  \
your_jar.jar  \
config-test.properties \
1>/home/hadoop/log/spark/app.log 2>&1 &
```