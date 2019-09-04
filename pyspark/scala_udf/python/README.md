Code Examples:
-----

The example here is from a stackoverflow question:
https://stackoverflow.com/questions/57102219/finding-non-overlapping-windows-in-a-pyspark-dataframe

The task is to add a sub-group-label `g` for the same id by every N seconds (column-`t`, N=6 in example codes).
there exist gaps between sub-groups, so every entry can be influenced by previous rows (ordered by `t`),
not a easy task for vectorized method. so we applied UDF to iterate the rows (using array by grouped with id).

```
df_new.show()
+---+----+---+
| id|   t|  g|
+---+----+---+
|  1| 0.0|  0|
|  1| 1.0|  0|
|  1| 4.0|  0|
|  1| 7.0|  1|
|  1|14.0|  2|
|  1|18.0|  2|
|  3| 0.0|  0|
|  3| 1.0|  0|
|  3| 1.5|  0|
|  3| 2.0|  0|
|  3| 3.5|  0|
|  3| 4.0|  0|
|  3| 6.0|  1|
|  3| 6.5|  1|
|  3| 7.0|  1|
|  3|11.0|  1|
|  3|14.0|  2|
|  3|18.0|  2|
|  3|20.0|  3|
|  3|24.0|  3|
+---+----+---+
```

Under Python, we can write a udf and set up it using pyspark.sql.functions.udf() function.
But this is less efficient since the data will switch/convert between Python interpretor 
and Scala interpreter which takes extra time. 
```
def set_group_label(arr, N):
    """This function set up a sub-group label based on the offset of 5
    from the first entries and rolling forward. gaps could exist between
    adjacent sub-groups. so a fixed Window funcion will not work. This
    function uses a Python UDF function to iterate through an array column
    which has the collect_list of the same group. the function will sort the
    list and check the distanct from the first_item in the same subgroup,
    when it exceeds the threshold `5`, increment the sub-group-id `g` and
    reset the first_item of the new sub-group.
    """
    g = 0; w0 = 0; new_arr = []
    threshold = N + 0.0001
    for x in sorted(arr):
        if x - w0 >= threshold:
            g += 1
            w0 = x
        new_arr.append({'t':x, 'g':g})
    return new_arr

udf_set_group_label = udf(lambda x: set_group_label_1(x, N), 'array<struct<t:double,g:int>>')
```

We discussed two methods how to implelment Scala UDF in Pyspark and the 3rd method using pyspark's function API:

+ Method-1 (myudf_method_1.py): in Scala, extend the class `org.apache.spark.sql.api.java.UDF2` (2 arguments UDF) and call the UDF with pyspark using: 
```
    spark.udf.registerJavaFunction(<udf_func_name>, <scala_class>, <return_type>)
```

+ Method-2 (myudf_method_2.py): in Scala, create a Functions object and register the udf function using `org.apache.spark.sql.SQLContext` and set up the UDF in pyspark using:
```
    spark._jvm.<package_name>.Functions.registerFunc(sqlContext._jsqlContext, <udf_func_name>)
```

+ Method-3: (myudf_method_1.py): using pyspark.sql.functions.udf()


**Note:**
 + pyspark.sql.functions.udf() only applied to Python, not available under SparkSQL context,
   thus you can not run pyspark udf using spark.sql() or selectExpr().
 + Scala-based UDF used in this project only applies to SparkSQL context.


