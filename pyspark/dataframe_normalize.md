# Normalize a Dataframe column #

To normalize a column with values from either a Python data structure, a JSON string or any delimited strings.

```
from pyspark.sql import SparkSession, functions as F
import json
""" create SparkSession """
spark = SparkSession.builder                          \
                    .master("spark://lexington:7077") \
                    .appName("pyspark-test1")         \
                    .getOrCreate()
```

## Python Data Structures ##

Functions availables:

+ pyspark.sql.functions.explode(col)
+ pyspark.sql.functions.posexplode(col)
+ pyspark.sql.Column.getItem(key)

Both F.explode() and F.posexplode() return a new Row for each array element 
or dictionary kay-value pair. F.posexplode() added one more column 'pos'(by default)
to identify the position of each exported Row.

Weakness: For dictionary, it will be good only if key names are consistent, arbitrary keys
will be an issue.

pyspark.sql.Column.getItem(key) can be used to retrieve any values from:
+ a list where key is position index
+ a dictionary where key is the dictionary key

```
"""below `recomm` field is a Python dictionary"""
In [122]: df2 = spark.createDataFrame(
    [({"5556867":1,"5801144":5,"7397596":21}, '20380109', [1,2,3,9]), ({"17359392":1,"17359394":3},'17359322', [3,4])],
    ("recomm", "items", "uid")
)

In [123]: df2.show(truncate=False)
+----------------------------------------------+--------+------------+
|recomm                                        |items   |uid         |
+----------------------------------------------+--------+------------+
|Map(5801144 -> 5, 7397596 -> 21, 5556867 -> 1)|20380109|[1, 2, 3, 9]|
|Map(17359394 -> 3, 17359392 -> 1)             |17359322|[3, 4]      |
+----------------------------------------------+--------+------------+

"""explode a simple dictionary with position of each element"""
In [124]: df2.select('items', F.posexplode('recomm').alias('pos', 'id', 'count')).show()
+--------+---+--------+-----+
|   items|pos|      id|count|
+--------+---+--------+-----+
|20380109|  0| 5801144|    5|
|20380109|  1| 7397596|   21|
|20380109|  2| 5556867|    1|
|17359322|  0|17359394|    3|
|17359322|  1|17359392|    1|
+--------+---+--------+-----+

"""explode a simple list"""
In [125]: df2.select('items', F.explode('uid').alias('id')).show()
+--------+---+
|   items| id|
+--------+---+
|20380109|  1|
|20380109|  2|
|20380109|  3|
|20380109|  9|
|17359322|  3|
|17359322|  4|
+--------+---+
```

For complexed data structures, you might have to do multiple F.explode()
by combining select() and/or withColumn()


**My experience:**

+ When the function returns a scalar, then use withColumn(), 
+ F.explode() returns multiple columns, thus have to use select()

**Note:**
+ only one F.posexplode() can be used in the same 'select()' statement, multiple posexplode will
  yield the following error:
```
AnalysisException: u'Only one generator allowed per select clause but found 2: posexplode(base1), posexplode(base3);'
```

Below example data are from the following stackoverflow link
REF: https://stackoverflow.com/questions/50714330/spliting-a-row-to-multiple-row-pyspark
```
In [18]: df.select('id', 'items_base').show(truncate=0)
+---+------------------------------------------------------------------------------+
|id |items_base                                                                    |
+---+------------------------------------------------------------------------------+
|0  |departmentcode__50~#~p99189h8pk0__10483~#~prod_productcolor__Dustysalmon Pink |
|1  |departmentcode__10~#~p99189h8pk0__10484~#~prod_productcolor__Dustysalmon Black|
|2  |departmentcode__60~#~p99189h8pk0__10485~#~prod_productcolor__Dustysalmon White|
+---+------------------------------------------------------------------------------+

In [19]: df.withColumn('base1', F.split('items_base', '~#~')) \
  .select('id', F.posexplode('base1')) \
  .withColumn('base2', F.split('col', '__')) \
  .select('id', 'pos', F.col('base2').getItem(0).alias('dept0'), F.col('base2').getItem(1).alias('att0')) \
  .show()

""" getItem(0) can be simplified by using the array indices, the following
modified verson yields the same results.
"""

In [20]: df.withColumn('base1', F.split('items_base', '~#~')) \
  .select('id', F.posexplode('base1')) \
  .withColumn('base2', F.split('col', '__')) \
  .select('id', 'pos', F.col('base2')[0].alias('dept0'), F.col('base2')[1].alias('att0')) \
  .show()
+---+---+-----------------+-----------------+
| id|pos|            dept0|             att0|
+---+---+-----------------+-----------------+
|  0|  0|   departmentcode|               50|
|  0|  1|      p99189h8pk0|            10483|
|  0|  2|prod_productcolor| Dustysalmon Pink|
|  1|  0|   departmentcode|               10|
|  1|  1|      p99189h8pk0|            10484|
|  1|  2|prod_productcolor|Dustysalmon Black|
|  2|  0|   departmentcode|               60|
|  2|  1|      p99189h8pk0|            10485|
|  2|  2|prod_productcolor|Dustysalmon White|
+---+---+-----------------+-----------------+

"""To merge the results from two columns with similar string layout"""
dfs = {}
for item in ['items_base', 'item_target']:
    dfs[item] = df.withColumn('base1', F.split(item, '~#~')) \
                  .select('id', F.posexplode('base1')) \
                  .withColumn('base2', F.split('col', '__')) \
                  .select('id', 'pos', F.col('base2')[0].alias('dept0'), F.col('base2')[1].alias('att0'))

dfs['item_target'].union(dfs['items_base']).show()

```
Note: `pyspark.sql.functions.struct(*cols)` might be used in data structures, use the dot notation
to refer to one of its elements, i.e. `m.device` in the following example:

```
df.select(F.min(F.struct('id', 'device', 'others')).alias('m')) \
  .select(F.col('m.device'))
```

## JSON string: ##

### F.from_json(col, schema, options={}) ###

TODO...

**Limitation:**  only takes structs or array of structs.


### F.get_json_object(col, jsonpath) ###

Need particular elements in a JSON string, one jsonpath a time
return null is the input json string is invalid. 

JSONPath online validator: [http://jsonpath.com/](http://jsonpath.com/)
```
s = """[{"1123798":"Other, poets"},{"1112194":"Poetry for kids"}]"""
```
For the above JSON string, JSONPath to get all keys are: `$[*].*`
[
  "Other, poets",
  "Poetry for kids"
]

However, this does not work for F.get_json_object() since it only accepts a scalar return.
Thus, this function is most likely useful only when you know the key of the dictionary and 
its return will always be a string.

```
In [901]: df2 = spark.createDataFrame(
    [(1123798, """{"key": ["Other, poets", "this one"], "key2": ["Poetry for kids"]}""")],
    ("catalogid", "catalogpath")
)
In [902]: df2.show(truncate=0)
+---------+------------------------------------------------------------------+
|catalogid|catalogpath                                                       |
+---------+------------------------------------------------------------------+
|1123798  |{"key": ["Other, poets", "this one"], "key2": ["Poetry for kids"]}|
+---------+------------------------------------------------------------------+

In [903]: df2.select('catalogid', F.get_json_object('catalogpath', '$.key').alias('desc_key')).show(truncate=0)
+---------+---------------------------+
|catalogid|desc_key                   |
+---------+---------------------------+
|1123798  |["Other, poets","this one"]|
+---------+---------------------------+


```
**Note:** The resulting `desc_key` is a String which can not be directly explode()

### F.json_tuple(col, *fields) ###
Create a new Row for a json column according to the given field names

**Con:** can handle only some simple Map, and the field names must be fixed

```
df2.select('catalogid', F.json_tuple('catalogpath', 'key', 'key2').alias('desc_key1', 'desc_key2')).show(truncate=0)
+---------+---------------------------+-------------------+
|catalogid|desc_key1                  |desc_key2          |
+---------+---------------------------+-------------------+
|1123798  |["Other, poets","this one"]|["Poetry for kids"]|
+---------+---------------------------+-------------------+
```

## JSON strings with Map having arbitrary keys ##
Use user-defined function and json.loads(), some example from stackoverflow:
+ https://stackoverflow.com/questions/50714330/spliting-a-row-to-multiple-row-pyspark
+ https://stackoverflow.com/questions/50704349/spark-split-and-parse-json-in-column
+ https://stackoverflow.com/questions/50683894/pyspark-explode-dict-in-column

```
In [842]:  %cpaste
@F.udf("array<map<string,string>>")
def parse(s):
    try:
        return json.loads(s)
    except:
        pass
--
In [843]: df2 = spark.createDataFrame(
    [(1123798, """[{"1123798":"Other, poets"},{"1112194":"Poetry for kids"}]""")],
    ("catalogid", "catalogpath")
)

In [844]: df2.show(truncate=False)
+---------+----------------------------------------------------------+
|catalogid|catalogpath                                               |
+---------+----------------------------------------------------------+
|1123798  |[{"1123798":"Other, poets"},{"1112194":"Poetry for kids"}]|
+---------+----------------------------------------------------------+
```

Convert the json string to Python data structure, then use F.explode() to 
normalize the data

```
In [845]: df2.withColumn("catalog", F.explode(parse("catalogpath"))) \
             .select("catalogid", F.explode("catalog").alias("cid","cdesc")) \
             .show()
+---------+----------+---------------+
|catalogid|catalog_id|   catalog_desc|
+---------+----------+---------------+
|  1123798|   1123798|   Other, poets|
|  1123798|   1112194|Poetry for kids|
+---------+----------+---------------+

In [846]: %cpaste
""" if only need the values:"""
@F.udf("array<string>")
def parse(s):
    return [ d[k] for d in json.loads(s) for k in d ]

In [847]: df2.select('catalogid', F.explode(parse('catalogpath'))).show(truncate=0)
+---------+---------------+
|catalogid|col            |
+---------+---------------+
|1123798  |Other, poets   |
|1123798  |Poetry for kids|
+---------+---------------+
```

## Arbitrary delimited strings ##

When the strings can not be identified by json.loads(), when you might have to use
Python split(), re.match() etc. and even rdd.flatMap(), rdd.map() to parse the data.

Performace: not able to use DF's optimization from Java

REF to [one example](rdd_map_to_Rows.py)

