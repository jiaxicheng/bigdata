## Apache Hive on Spark with Spark Standalone cluster (GlusterFS as storage) ##
Features
+ Running Apache Hive on spark engine
+ Using GlusterFS for storage (no HDFS)
+ Using Spark standalone cluster (no YARN)
+ Hive Metastore still requires the presence of Apache Hadoop, no need to run HDFS/YARN

### Setting on Spark cluster nodes:
+ manager and all workers have: 
  + spark-latest -> /data/hdfs/spark-2.2.1.no_hive
  + hadoop-latest -> /data/hdfs/hadoop-2.9.0
+ GFS is mounted on: file:///gfs/spark

### Softwares on Hive Metastore and HiveServer2:
+ hive-latest -> /data/hdfs/apache-hive-2.3.3-bin
+ hadoop-latest -> /data/hdfs/hadoop-2.9.0
