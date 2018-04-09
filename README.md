
## Targets: ##
* [x] Hive on mr - Apache Ignite caching + HDFS/YARN
* [x] Hive on tez - Apache HDFS/YARN
* [x] [Hive on spark - Spark on YARN](https://github.com/jiaxicheng/bigdata/tree/master/hive/hive_on_spark_yarn) 
* [x] [Hive on spark - Spark on Stand-alone cluster with HDFS](https://github.com/jiaxicheng/bigdata/tree/master/hive/hive_on_spark_standalone_hdfs)
* [ ] Hive on spark - Spark on Stand-alone cluster with GlusterFS
* [x] Spark SQL - Spark Stand-alone cluster with HDFS
* [x] [Spark SQL - Spark Stand-alone cluster with GlusterFS](https://github.com/jiaxicheng/bigdata/tree/master/spark/spark_thrift_on_gfs)

## Dependencies: ##
### Apache Hive ###
Metastore: 
 - hive -> /data/hdfs/apache-hive-2.3.3-bin
 - hadoop -> /data/hdfs/hadoop-2.9.0   

Hive2 Server:
 - must have metastore available
 - hive -> /data/hdfs/apache-hive-2.3.3-bin
 - hadoop -> /data/hdfs/hadoop-2.9.0

Apache Hive has strong tie with Hadoop. running any of its command without Hadoop will yield the following error:
```
Cannot find hadoop installation: $HADOOP_HOME or $HADOOP_PREFIX must be set or hadoop must be in the path
```
It's likely to run Hive well without running HDFS/YARN, but a hadoop installation MUST be accessible at a non-empty $HADOOP_HOME folder.

### Spark Cluster for Hive on spark ###
+ Spark must be compiled from the source without the flag: `-Phive`.
+ If -Phadoop-provided is supplied, then need to indlcude the following in environment variable and have apache hadoop physically accessible through $HADOOP_HOME
```
export SPARK_DIST_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

```

### Spark Cluster on its own ###
+ Spark: can download the precompiled bin-tar, so no need to have hadoop for:
  + Manager
  + Workers
  + Thrift Server

Note: make sure to use the spark version of beeline ($SPARK_HOME/bin/beeline).
