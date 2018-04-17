
## Targets: ##
* [x] Hive on mr - Apache Ignite caching
* [ ] Hive on mr with LLAP caching (TODO)
* [x] Hive on tez
* [ ] Hive on tez with LLAP caching (TODO)
* [x] [Hive on spark - Spark on YARN](https://github.com/jiaxicheng/bigdata/tree/master/hive/hive_on_spark_yarn) 
* [x] [Hive on spark - Spark on Stand-alone cluster with HDFS](https://github.com/jiaxicheng/bigdata/tree/master/hive/hive_on_spark_standalone_hdfs)
* [x] [Hive on spark - Spark on Stand-alone cluster with GlusterFS](https://github.com/jiaxicheng/bigdata/tree/master/hive/hive_on_spark_standalone_gfs)
* [x] Spark SQL - Spark Stand-alone cluster with HDFS
* [x] [Spark SQL - Spark Stand-alone cluster with GlusterFS](https://github.com/jiaxicheng/bigdata/tree/master/spark/spark_thrift_on_gfs)

## Dependencies: ##
### Apache Hive ###
Metastore: 
 - hive -> /data/hdfs/apache-hive-2.3.3-bin
 - hadoop -> /data/hdfs/hadoop-2.9.0   

HiveServer2:
 - must have metastore available
 - hive -> /data/hdfs/apache-hive-2.3.3-bin
 - hadoop -> /data/hdfs/hadoop-2.9.0

Apache Hive has strong tie with Hadoop. running any of its command without Hadoop will yield the following error:
```
Cannot find hadoop installation: $HADOOP_HOME or $HADOOP_PREFIX must be set or hadoop must be in the path
```
It's likely to run Hive well without running HDFS/YARN, but a hadoop installation MUST be accessible at a non-empty `$HADOOP_HOME` folder.

### Spark Cluster for Hive on spark ###
+ Spark must be compiled from the source without the flag: `-Phive`.
+ If -Phadoop-provided is supplied, then need to indlcude the following in environment variable and have apache hadoop physically accessible through $HADOOP_HOME
```
export SPARK_DIST_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

```
*Note:* hive.execution.engine, spark.master, spark.submit.deployMode can be adjusted at runtime. Do check these 
variables before running HQLs.

### Spark Cluster on its own ###
+ Spark: can download the precompiled bin-tar, so no need to have hadoop for:
  + Manager
  + Workers
  + Thrift Server

Note: make sure to use the spark version of beeline ($SPARK_HOME/bin/beeline).

### GlusterFS cluster ###
These are on the separate nodes, and managing details is not covered here.
+ GlusterFS provided distributed storage system that support also replication and high availabilities.
  Managing a GlusterFS cluster is 10 times simplier than managing a HDFS cluster.
+ To setup Gluster node on Centos 7:
  1. install softwares: 
  ```
    yum install centos-release-gluster38
    yum install glusterfs gluster-cli glusterfs-libs glusterfs-server
  ```
  2. firewall: just enable the service=glusterfs
