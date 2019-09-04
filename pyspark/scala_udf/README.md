Scala-based UDF function for pyspark
-----

### Procedures:

#### 1. Create the jar file containing Scala functions

##### Method-1:

(1) create a scala file {PROJECTROOT}/src/main/scala/com/jxc/spark/udf1.scala 
    where the {PROJECTROOT}/src/main/scala is base folder for scala source code
    `com/jxc/spark` corresponding to your package name. In the file, we create a 
    package `com.jxc.spark` and class `SetGroupLabel` and override its default 
    call method with the same code logic we set up using Python.

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
    cd "${PROJECTROOT}"
    sbt clean assembly
```

##### Method_2:

Simiar to Method_1, but without using the assembly plugins, skip the files under `project` folder 
and run the following command to build the jar file:
```
    cd "${PROJECTROOT}"
    sbt clean package
```

The jar file created from Method_2 is significantly smaller than the size from Method_1 and is
much faster to compile. Below is the final directory tree structure of this method:
```
.
├── build.sbt
└── src
    └── main
        └── scala
            └── com
                └── jxc
                    └── spark
                        └── udf2.scala
```

#### 2. Set up the UDF

After we have jar file, find the jar file under `{PROJECTROOT}/target/scala-<scala_version>/` and copy it 
to a proper location, and then run pyspark or spark-submit
```
    pyspark --jars /path/my_udf-assembly-1.0.jar

    spark-submit --jars /path/my_udf-assembly-1.0.jar my_pthon_file.py

```

In Python, we can do:

+ method_1: use spark.udf.registerJavaFunction() to reguster the Scala UDF 
+ method_2: use spark._jvm to set up the Scala UDF

and use the function with Spark SQL context, for example: df.selectExpr(), pyspark.sql.functions..expr()
or spark.sql() 

``` 
    # register Scala UDF with spark.udf.registerJavaFunction()
    spark.udf.registerJavaFunction('set_group_label_2', 'com.jxc.spark.SetGroupLabel2', 'array<struct<t:double,g:int>>')

    # use the named udf in Spark SQL, i.e. spark.sql(), selectExpr() or pyspark.sql.functions.expr()
    df.selectExpr('explode(set_group_label_2(arr, 6))')
    
```
