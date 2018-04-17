## Apache Hive on Spark with Spark Standalone cluster (GlusterFS as storage) ##
**Features:**
+ Running Apache Hive on spark engine
+ Using GlusterFS for storage (no HDFS)
+ Using Spark standalone cluster (no YARN)
+ Hive Metastore still requires the presence of Apache Hadoop, no need to run HDFS/YARN

**Issues:**
The current configuration is working only when the Worker is on the same server as Hive Server2,
all executors on other workers will failed with 'java.lang.NullPointerException' error. 
It's probably the limitation when setting Hive run on local server, Will need to check more details
when I have time later.

**Note:** all workers have the same `file:///gfs/spark` mount point and can access the required files with
the proper permission bits.

### Setting on Spark cluster nodes:
+ manager and all workers have: 
  + spark-latest -> /data/hdfs/spark-2.2.1.no_hive
  + hadoop-latest -> /data/hdfs/hadoop-2.9.0
+ GFS is mounted on: file:///gfs/spark

### Softwares on Hive Metastore and HiveServer2:
+ hive-latest -> /data/hdfs/apache-hive-2.3.3-bin
+ hadoop-latest -> /data/hdfs/hadoop-2.9.0


