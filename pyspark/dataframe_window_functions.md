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


## Example-2: Count frequency of similar phrases
[Spark - Group and Count By Similar Strings](https://stackoverflow.com/questions/50802444/spark-group-and-count-by-similar-strings-scala-or-pyspark)

```
source dataframe

In [195]: df2.show(10, 0)
+--------------------------+
|value                     |
+--------------------------+
|Apple                     |
|Banana                    |
|Tilamook Butter           |
|Gala Apple                |
|Pinto Beans               |
|Salt                      |
|Granny Smith Apple        |
|Generic Butter Beans Beans|
|Butter                    |
|Black Beans Apple         |
+--------------------------+

# to count the frequency of echo word
win = Window.partitionBy('word')

# for each original phrase, find the most frequently used word, put it on the top
# so that we can use F.row_number == 1 to retrieve it
win1 = Window.partitionBy('value').orderBy(F.col('count').desc())

# split phrase into words, count the words and then sort the result based on
# the original phrase, pick the word which is shown most frequently in the whole
# dataset and their counts
df3 = df2.withColumn('word', F.explode(F.split('value', '\s+')))\
   .withColumn('count', F.count('word').over(win)) \
   .select('word', 'count', F.row_number().over(win1).alias('top')) \
   .persist()

total = df3.where('top==1') \
           .dropDuplicates() \
           .drop('top') \
           .toPandas().set_index('word')['count'].to_dict()
print(total)
{u'Apple': 4, u'Banana': 1, u'Beans': 6, u'Butter': 4, u'Salt': 1}

# if multiple top words happen in the same phrase, the phrases are counted multiple times
# and thus such count should be cleared
# below list those which matched on more than one instance
over_counted = df3.where('(top > 1) and (count > 1)') \
                  .select('word') \
                  .rdd.flatMap(list) \
                  .collect()
print(over_counted)
[u'Butter', u'Apple', u'Beans', u'Butter']

# reduce count by one for each word shown in the list of over_counted
for k in over_counted:
    if k in total: total[k] -= 1

# print the resultset
print(total)
{u'Apple': 3, u'Banana': 1, u'Beans': 5, u'Butter': 2, u'Salt': 1}
```

Reference:
[1] [How to select the first row of each group](https://stackoverflow.com/questions/33878370/how-to-select-the-first-row-of-each-group)

*More to come...*

