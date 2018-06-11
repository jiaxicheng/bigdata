# PySpark.SQL Window functions 

Some common patterns using pyspark.sql Window Functions:
```
from pyspark.sql import Window

# default unbounded window
WindowSpec = Window.PartitionBy(*cols) \
                   .orderBy(*col)

# rowsBetween() to count number of rows
WindowSpec = Window.PartitionBy(*cols) \
                   .orderBy(*col) \         
                   .rowsBetween(start, end)      
                   
# rangeBetween can be used to count offset (useful for time series)
WindowSpec = Window.PartitionBy(*cols) \
                   .orderBy(*col) \         
                   .rangeBetween(start, end)
```

## orderBy(*col)

+ By default, the result is sorted by ascending order, you can specify columns in descending order
  with the following syntax:
```
   .orderBy(df.col1.desc())
```

## Window Specs

The window spec can be size-based (rowsBetween) or offset-based (rangeBetween). the later
one provides flexibility on the timeseries calculations.

### rangeBetween(start, end)

With rangeBetween(), the fields in orderBy() must be Integer or Long, example:
```
# 28 days before to now, timestamp has been casted to long 
# which is the seconds after '1970-01-01T00:00:00Z' 
# or Unix epoch(Unix time or POSIX time or Unix tamestamp)
window = Window.partitionBy("person_id", "dow")\
               .orderBy('timestamp')\
               .rangeBetween(-28*86400,-1)
               
df.withColumn('unix_ts', F.unix_timestamp(F.col('timestamp'), 'yyyy-MM-dd HH:mm:ss.SSSSSS')) \
  .withColumn('dow_count', F.count('dow').over(window))
```
**Note:**

  1. Use F.unix_timestamp(.., 'yyyy-MM-dd HH:mm:ss.SSSSSS') to get better datetime precision.
     `.astype('Timestamp').cast('long')` is not enough

  2. Use cast() or astype() (alias of cast) to convert datatype

### rowsBetween(start, end)
with rowsBetween(), you can use String, Timestamp etc in orderBy()
and the window will be counted by number of rows.

### Available Window functions:
  + Ranking functions: rank(), dense_rank(), percent_rank(), ntile() and row_number()
  + Analytic functions: come_dist(), first_value(), last_value(), lag() and lead()
  + General: count(), max(), min(), mean()

+ Common Ranges:
  + currentRow: 0
  + unboundedPreceding, unboundedFollowing, which can be setup with the following Python code:
```
  .rangeBetween(-sys.maxsize, sys.maxsize)
```

## Example-1: Select the first row of each groupby
Below example to retrieve the `device`name with longest name for each grouped `id`

**Method-1: Using Window:**

    winSpec = Window.partitionBy('id', 'name').orderBy(F.col(length).desc())
    df.withColumn('length', F.length('col_name')) \
      .withColumn('length', F.row_number().over(winSpec).orderBy('length')) \
      .where('length == 1') \
      .drop('length') \
      .show(10, False)
    
**Method-2: using groupby and F.struct():**

    df.withColumn('length', F.length('col_name')) \
      .groupby('id') \
      .agg(F.max(F.struct('length', 'device')).alias('m')) \
      .select('id', F.col('m.device'))
      .show(10, False)

Note: windows and F.first_number() are frequently used in adding a sequence number to a groupby list.

Reference:
[1] [How to select the first row of each group](https://stackoverflow.com/questions/33878370/how-to-select-the-first-row-of-each-group)

*More to come...*

