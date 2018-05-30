#!/usr/bin/env python
"""
The task is to convert a list of key-value pairs into spark dataframe, sample text:
---
name:Pradnya,IP:100.0.0.4, college: SDM, year:2018
name:Ram, IP:100.10.10.5, college: BVB, semester:IV, year:2018 

REF:https://stackoverflow.com/questions/50579452/reading-a-csv-file-into-pyspark-that-contains-the-keyvalue-pairing-such-that-k

What I learnt from this task:
+ pyspark.sql.Row objects can be used to create DataFrame.
+ The named arguments in Row() constructor are sorted internally by keys
  thus using a OrderedDict() does not help. 
+ All columns must be shown in the named arguments, otherwise will yield error:
  `Input row doesn't have expected number of values required by the schema.`
+ Use split() + strip() instead of re.split(), avoid regex whenever possible

The proposed solution below could applied only if:
+ All field names are known and can be predefined. important to create Row() object
+ keys and values do not contain embedded delimiters ':' and ','
  complexed structures might have to rely on specific parsers instead of string split()
  but the method still applies: Using rdd.map() to convert text into Row() objects.

"""
from pyspark.sql import SparkSession, Row
from string import lower

# create SparkSession
spark = SparkSession.builder                          \
                    .master("spark://lexington:7077") \
                    .appName("pyspark-test1")         \
                    .getOrCreate()

# initialize the RDD
rdd = spark.sparkContext.textFile("key-value-file")

# define a list of all field names
columns = ['name', 'IP', 'College', 'Semester', 'year']

# set Row object
def setRow(x):
    # convert line into key/value tuples. strip spaces and lowercase the `k`
    z = dict((lower(k.strip()), v.strip()) for e in x.split(',') for k,v in [ e.split(':') ])

    # make sure all columns shown in the Row object
    return Row(**dict((c, z[c] if c in z else None) for c in map(lower, columns)))

# map lines to Row objects and then convert the result to dataframe
df = rdd.map(setRow).toDF()
df.show()
#+-------+-----------+-------+--------+----+
#|college|         ip|   name|semester|year|
#+-------+-----------+-------+--------+----+
#|    SDM|  100.0.0.4|Pradnya|    null|2018|
#|    BVB|100.10.10.5|    Ram|      IV|2018|
#+-------+-----------+-------+--------+----+

# more operations on df
spark.stop()
