"""
https://stackoverflow.com/questions/57102219/finding-non-overlapping-windows-in-a-pyspark-dataframe

method-2: using spark._jvm to set up Scala UDF in pyspark:

    spark-submit --jars my_udf_3_2.11-1.01.jar myudf_method_2.py

"""
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import collect_list, expr

def spark_udf_set_group_label():
    sqlContext = SQLContext(spark.sparkContext)
    spark._jvm.com.jxc.spark.Functions.registerFunc(sqlContext._jsqlContext, "set_group_label_2")
    df_new = df.groupby('id') \
               .agg(collect_list('t').alias('arr')) \
               .withColumn('arr', expr('explode(set_group_label_2(arr, 6))')) \
               .select('id', 'arr.*')
    df_new.show()


if __name__ == "__main__":

    spark = SparkSession.builder.appName("ScalaUDFtoPyspark").getOrCreate()

    d = [ (1,0), (1,1), (1,4), (1,7), (1,14), (1,18)
        , (2,5), (2,20), (2,21)
        , (3,0), (3,1), (3,1.5), (3,2), (3,3.5), (3,4), (3,6)
        , (3,6.5), (3,7), (3,11), (3,14), (3,18), (3,20), (3,24)
        , (4,0), (4,1), (4,2), (4,6), (4,7)
    ]   

    df = spark.createDataFrame([(int(x[0]), float(x[1])) for x in d], ['id', 't'])

    spark_udf_set_group_label()

    spark.stop()
