## Apache Hive on Spark with Spark Standalone cluster (GlusterFS as storage) ##

* [ ] TODO: this is not yet completed.

### Current Setting on Spark cluster nodes:
+ manager and all workers have: 
  + spark-latest -> /data/hdfs/spark-2.2.1.no_hive
  + hadoop-latest -> /data/hdfs/hadoop-2.9.0
+ GFS is mounted on: file:///gfs/spark

### Softwares on Hive Metastore and HiveServer2:
+ hive-latest -> /data/hdfs/apache-hive-2.3.3-bin
+ hadoop-latest -> /data/hdfs/hadoop-2.9.0

---
### Current issues: 
error on executor worker node after the application was submitted:
```
[hdfs@borders work]$ tree app-20180408120628-0002
app-20180408120628-0002
└── 1
    ├── hive-exec-2.3.3.jar
    ├── stderr
    └── stdout

ERROR (snippet from stderr): 
18/04/08 12:07:01 INFO broadcast.TorrentBroadcast: Started reading broadcast variable 0
18/04/08 12:07:01 INFO client.TransportClientFactory: Successfully created connection to /192.168.1.23:32025 after 2 ms (0 ms spent in bootstraps)
18/04/08 12:07:01 INFO memory.MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 76.6 KB, free 413.5 MB)
18/04/08 12:07:01 INFO broadcast.TorrentBroadcast: Reading broadcast variable 0 took 55 ms
18/04/08 12:07:01 INFO Configuration.deprecation: mapred.task.is.map is deprecated. Instead, use mapreduce.task.ismap
18/04/08 12:07:01 INFO memory.MemoryStore: Block broadcast_0 stored as values in memory (estimated size 979.7 KB, free 412.6 MB)
18/04/08 12:07:02 INFO conf.HiveConf: Found configuration file file:/home/hdfs/hive-latest/conf/hive-site.xml
18/04/08 12:07:02 INFO exec.Utilities: PLAN PATH = file:/tmp/hive/hdfs/8b0e4010-d334-4a77-971f-84ecf6ec4a9c/hive_2018-04-08_12-06-21_369_5767759768167965092-2/-mr-10004/d5083f55-06ed-4ba1-b259-c3d2195db1d6/map.xml
18/04/08 12:07:03 ERROR executor.Executor: Exception in task 1.0 in stage 0.0 (TID 1)
java.lang.NullPointerException
    at org.apache.hadoop.hive.ql.io.HiveInputFormat.init(HiveInputFormat.java:408)
    at org.apache.hadoop.hive.ql.io.HiveInputFormat.pushProjectionsAndFilters(HiveInputFormat.java:665)
    at org.apache.hadoop.hive.ql.io.HiveInputFormat.pushProjectionsAndFilters(HiveInputFormat.java:658)
    at org.apache.hadoop.hive.ql.io.CombineHiveInputFormat.getRecordReader(CombineHiveInputFormat.java:692)
    at org.apache.spark.rdd.HadoopRDD$$anon$1.liftedTree1$1(HadoopRDD.scala:251)
    at org.apache.spark.rdd.HadoopRDD$$anon$1.<init>(HadoopRDD.scala:250)
    at org.apache.spark.rdd.HadoopRDD.compute(HadoopRDD.scala:208)
    at org.apache.spark.rdd.HadoopRDD.compute(HadoopRDD.scala:94)
    at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:323)
    at org.apache.spark.rdd.RDD.iterator(RDD.scala:287)
    at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:38)
    at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:323)
    at org.apache.spark.rdd.RDD.iterator(RDD.scala:287)
    at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:96)
    at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:53)
    at org.apache.spark.scheduler.Task.run(Task.scala:108)
    at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:338)
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    at java.lang.Thread.run(Thread.java:748)
18/04/08 12:07:03 INFO executor.CoarseGrainedExecutorBackend: Got assigned task 5
18/04/08 12:07:03 INFO executor.Executor: Running task 1.3 in stage 0.0 (TID 5)
18/04/08 12:07:03 INFO rdd.HadoopRDD: Input split: Paths:/gfs/spark/dfs/iotop/sgrove/2017-05-12/2017-05-12.txt:0+5416727,/gfs/spark/dfs/iotop/sgrove/2017-05-13/iotop.1494662286088.txt.tmp:0+29200,/gfs/spark/dfs/iotop/sgrove/2017-05-13/2017-05-13.txt:0+2364145,/gfs/spark/dfs/iotop/sgrove/2017-06-09/2017-06-09.txt:0+1083341,/gfs/spark/dfs/iotop/sgrove/2017-06-09/iotop.1497044489945.txt.tmp:0+21499,/gfs/spark/dfs/iotop/sgrove/2017-06-09/iotop.1497041829158.txt.tmp:0+5141,/gfs/spark/dfs/iotop/sgrove/2017-06-10/iotop.1497067213527.txt.tmp:0+11759,/gfs/spark/dfs/iotop/sgrove/2017-06-10/2017-06-10.txt:0+2167156,/gfs/spark/dfs/iotop/sgrove/2017-06-13/2017-06-13.txt:0+1479708,/gfs/spark/dfs/iotop/sgrove/2017-06-14/2017-06-14.txt:0+2237847,/gfs/spark/dfs/iotop/sgrove/2017-06-15/2017-06-15.txt:0+3629484,/gfs/spark/dfs/iotop/sgrove/2017-06-16/2017-06-16.txt:0+3694560,/gfs/spark/dfs/iotop/sgrove/2017-06-17/2017-06-17.txt:0+3395576,/gfs/spark/dfs/iotop/sgrove/2017-06-18/2017-06-18.txt:0+3403274,/gfs/spark/dfs/iotop/sgrove/2017-06-19/2017-06-19.txt:0+2466310,/gfs/spark/dfs/iotop/sgrove/2017-07-05/2017-07-05.txt:0+1912777,/gfs/spark/dfs/iotop/sgrove/2017-07-06/2017-07-06.txt:0+4030449,/gfs/spark/dfs/iotop/sgrove/2017-07-07/2017-07-07.txt:0+3854325,/gfs/spark/dfs/iotop/sgrove/2017-07-08/2017-07-08.txt:0+3760136,/gfs/spark/dfs/iotop/sgrove/2017-07-09/2017-07-09.txt:0+3956763,/gfs/spark/dfs/iotop/sgrove/2017-07-10/2017-07-10.txt:0+3928105,/gfs/spark/dfs/iotop/sgrove/2017-07-11/2017-07-11.txt:0+2047956,/gfs/spark/dfs/iotop/sgrove/2017-08-08/iotop.1502208534121.txt.tmp:0+8887,/gfs/spark/dfs/iotop/sgrove/2017-08-08/2017-08-08.txt:0+2752313,/gfs/spark/dfs/iotop/sgrove/2017-08-09/iotop.1502251241791.txt.tmp:0+39329,/gfs/spark/dfs/iotop/sgrove/2017-08-09/2017-08-09.txt:0+1451686InputFormatClass: org.apache.hadoop.mapred.TextInputFormat

```



