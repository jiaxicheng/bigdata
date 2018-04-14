## Apache Hive on Spark with Spark Standalone cluster (HDFS as storage) ##

Apache Hive must have the following directives in hive-site.xml, or add these into spark-default.conf
under the same folder as hive-site.xml under $HIVE_CONF_DIR (i.e. $HIVE_HOME/conf)
```
  <property>
    <name>spark.master</name>
    <value>spark://lexington:7077</value>
  </property>
  <property>
    <name>spark.submit.deployMode</name>
    <value>client</value>    
  </property>
```

*Note:* the following is setting the cluster mode(port 6066 instead of 7077), this will need to have 
apache-hive installed on all worker nodes which is not flexible.
```
  <property>
    <name>spark.master</name>
    <value>spark://lexington:6066</value>
  </property>
```

### Softwares required:###
* Spark Cluster nodes (both Manager and Workers):
  - spark-latest -> /data/hdfs/spark-2.2.1.no_hive (Worker)

* Hive2 Server:
  - spark-latest -> /data/hdfs/spark-2.2.1.no_hive
  - hive-latest -> /data/hdfs/apache-hive-2.3.3-bin
  - hadoop-latest -> /data/hdfs/hadoop-2.9.0

* Hive Metastore node:
  - hive-latest -> /data/hdfs/apache-hive-2.3.3-bin
  - hadoop-latest -> /data/hdfs/hadoop-2.9.0


---
```
Error: Error while compiling statement: FAILED: SemanticException Failed to
get a spark session: org.apache.hadoop.hive.ql.metadata.HiveException: Failed
to create spark client. (state=42000,code=40000)
```
*Solution*: Likely the compatibility issue between spark versions. When compiling spark, do NOT use `-Phive` 

---
```
ERROR: java.nio.file.NoSuchFileException: /data/hdfs/apache-hive-2.3.3-bin/lib/hive-exec-2.3.3.jar
```
*Solution*: set `spark.submit.deployMode = client`

In such case, the remote worker will download the jar file(hive-exec-2.3.3.jar) from the Manager node
at runtime before it starts running its main task. 

On the other hand, you can also install Hive on all worker nodes, in such case using the default 
`cluster` deployMode will be more efficient.
