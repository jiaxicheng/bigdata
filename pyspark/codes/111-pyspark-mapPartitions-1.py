"""
https://stackoverflow.com/questions/58294966/how-to-identify-peoples-relationship-based-on-name-address-and-then-assign-a-s
    
Questions:
(1) big file 30 GB
(2) want to generate sequential numric IDs based on a unique constraint
    
Sample data: `partition-1.txt`
---
D,FNAME,MNAME,LNAME,GENDER,DOB,snapshot,Address
2,66M,J,Rock,F,1995,201211.0,J
3,David,HM,Lee,M,1991,201211.0,J
6,66M,,Rock,F,1990,201211.0,J
6,66M,,Rek,F,1991,201211.0,J
6,66M,,Rek,F,1992,201211.0,J
6,66M,,Rock,F,1990,201211.0,J
0,David,H M,Lee,M,1990,201211.0,B
3,Marc,H,Robert,M,2000,201211.0,C
6,Marc,M,Robert,M,1988,201211.0,C
6,Marc,MS,Robert,M,2000,201211.0,D

df.show()                                                                                                          
+---+-----+-----+------+------+----+--------+-------+
|  D|FNAME|MNAME| LNAME|GENDER| DOB|snapshot|Address|
+---+-----+-----+------+------+----+--------+-------+
|  2|  66M|    J|  Rock|     F|1995|201211.0|      J|
|  3|David|   HM|   Lee|     M|1991|201211.0|      J|
|  6|  66M| null|  Rock|     F|1990|201211.0|      J|
|  6|  66M| null|   Rek|     F|1991|201211.0|      J|
|  6|  66M| null|   Rek|     F|1992|201211.0|      J|
|  6|  66M| null|  Rock|     F|1990|201211.0|      J|
|  0|David|  H M|   Lee|     M|1990|201211.0|      B|
|  3| Marc|    H|Robert|     M|2000|201211.0|      C|
|  6| Marc|    M|Robert|     M|1988|201211.0|      C|
|  6| Marc|   MS|Robert|     M|2000|201211.0|      D|
+---+-----+-----+------+------+----+--------+-------+

Run it: copy the file partition-1.txt to file:///home/xicheng/test/partition-1.txt
        and then run the following command:

   spark-submit pending_post/_111-pyspark-mapPartitions.py

"""
    
from pyspark.sql import Window, SparkSession, Row
from pyspark.sql.functions import coalesce, sum as _sum, col, max as _max, lit

# function to iterate through the sorted list of elements in the same partition
# assign idx in partition based on Address and LNAME
def func(sid, it):
    idx = 0
    lname = None; address = None
    for row in sorted(it, key=lambda x: (x.LNAME, x.Address)):
        if lname and (row.LNAME != lname or row.Address != address): idx += 1
        yield Row(sid=sid, idx=idx, **row.asDict())
        lname = row.LNAME
        address = row.Address
    

if __name__ == '__main__':
   
    spark = SparkSession.builder \
                        .master('local[*]') \
                        .appName('test') \
                        .getOrCreate()

    df = spark.read.csv('file:///home/xicheng/test/partition-1.txt', header=True)
    
    """ repartition based on 'LNAME' and 'Address' and generate spark_partiion_id
    then run mapPartitions() function and create in-partition idx
    """
    df1 = df.repartition('LNAME', 'Address') \
            .rdd.mapPartitionsWithIndex(func, preservesPartitioning=True) \
            .toDF()
    
    # get number of unique rows (based on Address+LNAME) which is max_idx+1
    # and then grab the running SUM of this rcnt 
    # the new df should be small and just cache it
    w1 = Window.partitionBy().orderBy('sid').rowsBetween(Window.unboundedPreceding,-1)
    
    df2 = df1.groupby('sid') \
             .agg((_max('idx')+1).alias('cnt')) \
             .withColumn('rcnt', coalesce(_sum('cnt').over(w1),lit(0))) \
             .cache()
    df2.show()
    #+---+---+----+                                                                  
    #|sid|cnt|rcnt|
    #+---+---+----+
    #|  0|  3|   0|
    #|  1|  1|   3|
    #|  2|  1|   4|
    #|  4|  1|   5|
    #+---+---+----+
    
    """join df1 with df2 and create id = idx + rcnt"""
    df_new = df1.join(df2, on=['sid']).withColumn('id', col('idx')+col('rcnt')) \

    df_new.show()
    #+---+-------+---+----+-----+------+------+-----+---+--------+---+----+---+      
    #|sid|Address|  D| DOB|FNAME|GENDER| LNAME|MNAME|idx|snapshot|cnt|rcnt| id|
    #+---+-------+---+----+-----+------+------+-----+---+--------+---+----+---+
    #|  0|      B|  0|1990|David|     M|   Lee|  H M|  0|201211.0|  3|   0|  0|
    #|  0|      J|  3|1991|David|     M|   Lee|   HM|  1|201211.0|  3|   0|  1|
    #|  0|      D|  6|2000| Marc|     M|Robert|   MS|  2|201211.0|  3|   0|  2|
    #|  1|      C|  3|2000| Marc|     M|Robert|    H|  0|201211.0|  1|   3|  3|
    #|  1|      C|  6|1988| Marc|     M|Robert|    M|  0|201211.0|  1|   3|  3|
    #|  2|      J|  6|1991|  66M|     F|   Rek| null|  0|201211.0|  1|   4|  4|
    #|  2|      J|  6|1992|  66M|     F|   Rek| null|  0|201211.0|  1|   4|  4|
    #|  4|      J|  2|1995|  66M|     F|  Rock|    J|  0|201211.0|  1|   5|  5|
    #|  4|      J|  6|1990|  66M|     F|  Rock| null|  0|201211.0|  1|   5|  5|
    #|  4|      J|  6|1990|  66M|     F|  Rock| null|  0|201211.0|  1|   5|  5|
    #+---+-------+---+----+-----+------+------+-----+---+--------+---+----+---+

    """sid, cnt, idx, rcnt, id could be different on each run based on how the
    Rows are partitioned, the end result shuold be a unique(based on two fields)
    and consecutive id are generated for the dataframe
    """    
    df_new = df_new.drop('sid', 'idx', 'rcnt')

    spark.stop()
