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


