Hive Metastore should be managed separated from HiveServer2 and other Hadoop applications, 
preferred start in a different server. so the credential to access metastore is not shared by 
other server configurations.

Need to have Apache Hadoop in the same system accessible with $HADOOP_HOME, no need to run HDFS or YRAN.

REF: [Official Doc](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+3.0+Administration)

For MySQL-backend, the metastore upgrade from Hive 2.1.0 to Hive 2.3.0 can be refer to 
the [link](https://github.com/apache/hive/tree/master/metastore/scripts/upgrade/mysql)

