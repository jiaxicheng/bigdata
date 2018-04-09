## Running Spark SQL under Spark thriftserver over GlusterFS ##

### Softwares required: ###
1. Spark must be compiled with -Phive and -Phive-thriftserver(prefer -Phadoop-2.7 so no 
   need to have hadoop in the same system) On the manager node and all workers:
```
   spark-latest -> /data/hdfs/spark-2.2.1-bin-hadoop2.7 
```   
2. Hive Metastore server must have hadoop available on the same server(no need to run HDFS or YARN)

------
### Configurations: ###
1. For the Spark Cluster:
All servers must have the following two files under $SPARK_HOME/conf/
- spark-default.conf
- spark-env.sh            <-- export SPARK_MASTER_HOST=lexington
`slaves` file is used only used on the Manager node

2. On the Manager node, run the following command to start the cluster:
```
    $SPARK_HOME/sbin/start-all.sh
```
3. On the Spark Thrift server, have the hive-site configuration for the thrift server
`$SPARK_HOME/conf/hive-site.xml` and run the following command to start the thrift server
```
    $SPARK_HOME/sbin/start-thriftserver.sh
```
4. Client side, use the spark version beeline: 
```
    SPARK_HOME/bin/beeline -u jdbc:hive2://lexington:10000/gfs
```
