Scala-based UDF function for pyspark
-----

Convert the following pyspark UDF to Scala UDF:
```
@F.udf('array<struct<t:double,g:int>>')
def set_group_label(arr):
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
    for x in sorted(arr):
        if x - w0 >= 5.0001:
            g += 1
            w0 = x
        new_arr.append({'t':x, 'g':g})
    return new_arr
```

Procedures:

(1) create a scala file {PROJECTROOT}/src/main/scala/com/jxc/spark/udf1.scala containing the 
    class `com.jxc.spark.SetGroupLabel` to setup the Scala-based UDF function

(2) create {PROJECTROOT}/build.sbt file to add basic project information and dependencies

(3) create {PROJECTROOT}/project/assembly.sbt to add assembly plugins 

The files/directories under the {PROJECTROOT} should have the following structure:
```
.
├── build.sbt
├── project
│   ├── assembly.sbt
│   └── readme.txt
└── src
    └── main
        └── scala
            └── com
                └── jxc
                    └── spark
                        └── udf1.scala
```

(4) Run the following command to compile the jar file
```
    sbt clean assembly
```

(5) copy the jar file to proper location
```
    {PROJECTROOT}/target/scala-<scala_version>/<project_name>-assembly-<project_version>.jar
```

(6) run pyspark or spark-submit
```
    pyspark --jars /path/to/my_udf-assembly-1.0.jar
```

(7) use spark.udf.registerJavaFunction() to reguster the Scala UDF and use it with Spark SQL
    context, for example: df.selectExpr()

``` 
    # register Scala UDF with spark.udf.registerJavaFunction()
    spark.udf.registerJavaFunction('set_group_label_2', 'com.jxc.spark.SetGroupLabel2', 'array<struct<t:double,g:int>>')

    # use the named udf in Spark SQL, i.e. spark.sql(), selectExpr() or pyspark.sql.functions.expr()
    df.selectExpr('explode(set_group_label_2(arr, 6))')
    
```
