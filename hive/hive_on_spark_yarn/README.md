== Hive on spark with yarn ==

Spark is running on top of the hadoop cluster and managed by yarn(resource/node manager)

Softwares required on various nodes:
* Apache hadoop cluster nodes:
  - hadoop-latest -> /data/hdfs/hadoop-2.9.0
  - zookeeper -> /data/hdfs/zookeeper-3.4.11

* HiveServer2 node:
  - hive-latest -> /data/hdfs/apache-hive-2.3.3-bin
  - hadoop-latest -> /data/hdfs/hadoop-2.9.0
  - spark-latest -> /data/hdfs/spark-2.2.1.no_hive

* Hive Metastore node:
  - hive-latest -> /data/hdfs/apache-hive-2.3.3-bin
  - hadoop-latest -> /data/hdfs/hadoop-2.9.0

Note:
* Spark is only needed on the same server running the Apache Hive2 server
* Spark must be compiled without -Phive

Note: for spark comipled with -Phadoop-provided, need to add the following environment variable in the shell:
```
export SPARK_DIST_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

```

For spark_on_hive, add the following directive into $HADOOP_HOME/etc/hadoop/yarn-site.xml
REF:[Officeal Doc](https://cwiki.apache.org/confluence/display/Hive/Hive+on+Spark%3A+Getting+Started)
```
<property>
  <name>yarn.resourcemanager.scheduler.class</name>
  <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>
</property>
```

