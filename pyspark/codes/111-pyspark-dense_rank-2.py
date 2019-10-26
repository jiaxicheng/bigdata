"""
https://stackoverflow.com/questions/58294966/how-to-identify-peoples-relationship-based-on-name-address-and-then-assign-a-s
    
Questions:
(1) big file 30 GB
(2) want to generate sequential numric IDs based on a unique constraint

Method-2: use dense_rank to calculate the in-partition idx
    
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

   spark-submit pending_post/_111-pyspark-dense_rank.py

"""
    
from pyspark.sql import Window, SparkSession, Row
from pyspark.sql.functions import (regexp_replace, concat_ws, upper, trim, expr, split,
    coalesce, lit, spark_partition_id, dense_rank, broadcast, sum as fsum, col, max as fmax)

# function to iterate through the sorted list of elements in the same partition
# assign idx in partition based on Address and LNAME
def func_normalized(partition_id, it):
    idx, pkey = (0, None)
    for row in sorted(it, key=lambda x: x.p_key):
        if pkey and (row.p_key != pkey): idx += 1
        yield Row(partition_id=partition_id, idx=idx, **{ k:v for k,v in row.asDict().items() if k != 'p_key' })
        pkey = row.p_key


if __name__ == '__main__':
   
    spark = SparkSession.builder \
                        .master('local[*]') \
                        .appName('test') \
                        .config("spark.sql.shuffle.partitions", 10) \
                        .getOrCreate()

    df = spark.read.csv('file:///home/xicheng/test/partition-1.txt', header=True)
    
    # normalize the LNAME/Address, uppercase(), regex_replace etc
    # (1) convert NULL to '': coalesce(col, '')
    # (2) concatenate LNAME and Address using NULL char '\x00' or '\0'
    # (3) uppercase: upper(text)
    # (4) remove all regexp_replace(text, r'[^\x00\w\s]', '')
    # (5) convert consecutive whitespaces to a SPACE: regexp_replace(text, r'\s+', ' ')
    # (6) trim leading/trailing spaces: trim(text)
    df = (df.withColumn('p_key', 
        trim(
          regexp_replace(
            regexp_replace(
              upper(
                concat_ws('\x00', coalesce('LNAME', lit('')), coalesce('Address', lit('')))
              ),
              r'[^\x00\s\w]+',
              ''
            ), 
            r'\s+', 
            ' '
          )
        )
    ))

    # tweak the number of repartitioning N based on realy data size
    N = 5

    # use dense_rank to calculate the in-partition idx
    w1 = Window.partitionBy('partition_id').orderBy('p_key')
    df1 = df.repartition(N, 'p_key') \
            .withColumn('partition_id', spark_partition_id()) \
            .withColumn('idx', dense_rank().over(w1))

    
    # get number of unique rows (based on Address+LNAME) which is max_idx+1
    # and then grab the running SUM of this cnt -> rcnt
    # the new df should be small and cache it
    # partition_id: spark partition id
    # idx: calculated in-partition id
    # cnt: number of unique ids in the same partition fmax('idx')
    # rcnt: starting_id for a partition(something like a running count): coalesce(fsum('cnt').over(w1),lit(0))
    # w1: WindowSpec to calculate the starting_id rcnt
    w2 = Window.partitionBy().orderBy('partition_id').rowsBetween(Window.unboundedPreceding,-1)
    
    df2 = df1.groupby('partition_id') \
             .agg((fmax('idx')).alias('cnt')) \
             .withColumn('rcnt', coalesce(fsum('cnt').over(w2),lit(0))) 

    df2.show()
    #+------------+---+----+                                                         
    #|partition_id|cnt|rcnt|
    #+------------+---+----+
    #|           0|  3|   0|
    #|           1|  1|   3|
    #|           2|  1|   4|
    #|           4|  1|   5|
    #+------------+---+----+

    
    """join df1 with df2 and create id = idx + rcnt"""
    df_new = df1.join(broadcast(df2), on=['partition_id']).withColumn('id', col('idx')+col('rcnt')) 

    df_new.show()
    #+------------+-------+---+----+-----+------+------+-----+---+--------+---+----+---+
    #|partition_id|Address|  D| DOB|FNAME|GENDER| LNAME|MNAME|idx|snapshot|cnt|rcnt| id|
    #+------------+-------+---+----+-----+------+------+-----+---+--------+---+----+---+
    #|           0|      B|  0|1990|David|     M|   Lee|  H M|  0|201211.0|  3|   0|  0|
    #|           0|      J|  3|1991|David|     M|   Lee|   HM|  1|201211.0|  3|   0|  1|
    #|           0|      D|  6|2000| Marc|     M|Robert|   MS|  2|201211.0|  3|   0|  2|
    #|           1|      C|  3|2000| Marc|     M|Robert|    H|  0|201211.0|  1|   3|  3|
    #|           1|      C|  6|1988| Marc|     M|Robert|    M|  0|201211.0|  1|   3|  3|
    #|           2|      J|  6|1991|  66M|     F|   Rek| null|  0|201211.0|  1|   4|  4|
    #|           2|      J|  6|1992|  66M|     F|   Rek| null|  0|201211.0|  1|   4|  4|
    #|           4|      J|  2|1995|  66M|     F|  Rock|    J|  0|201211.0|  1|   5|  5|
    #|           4|      J|  6|1990|  66M|     F|  Rock| null|  0|201211.0|  1|   5|  5|
    #|           4|      J|  6|1990|  66M|     F|  Rock| null|  0|201211.0|  1|   5|  5|
    #+------------+-------+---+----+-----+------+------+-----+---+--------+---+----+---+

    """partition_id, cnt, idx, rcnt, id could be different on each run based on how the
    Rows are partitioned, the end result shuold be a unique(based on two fields)
    and consecutive id are generated for the dataframe
    """    
    df_new = df_new.drop('partition_id', 'idx', 'cnt', 'rcnt')

    spark.stop()
