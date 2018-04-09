#The dependencies:#


## Spark Cluster: ##
Manager: 
  spark-latest -> /data/hdfs/spark-2.2.1
  no hadoop, no hive
  Note: a little strange when using the verion 'spark-2.2.1-bin-hadoop2.7' which keep searching namenode.

Workers: 
  spark-latest -> /data/hdfs/data/hdfs/spark-2.2.1-bin-hadoop2.7
  no hive
  Note: hadoop jars were included in the precompiled version: spark-2.2.1-bin-hadoop2.7
  
Thrift Server: 
  spark-latest -> /data/hdfs/spark-2.2.1
  no hadoop, no hive

$SPARK_HOM/bin/beeline
  spark-latest -> /data/hdfs/spark-2.2.1
  no hadoop, no hive

Spark SQL can pretty much run w/o Hadoop. need to test out the Workers though


## Apache Hive ##
Metastore: 
  hive-latest -> /data/hdfs/apache-hive-2.3.3-bin
  hadoop-latest -> /data/hdfs/hadoop-2.9.0   
  ERROR w/o Hadoop: Cannot find hadoop installation: $HADOOP_HOME or $HADOOP_PREFIX must be set or hadoop must be in the path

Hive2 Server:
  must have metastore available
  hive-latest -> /data/hdfs/apache-hive-2.3.3-bin
  hadoop-latest -> /data/hdfs/hadoop-2.9.0
  
Note: Hadoop does not need to be running, but $HADOOP_HOME and foler must physically exist
  
$HIVE_HOME/bin/beeline: the same as all others

Summary: Apache Hive has strong tie with Hadoop. no need to run HDFS/YARN, but a hadoop installation MUST be accessible at a non-empty $HADOOP_HOME


