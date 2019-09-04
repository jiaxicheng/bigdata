"""
https://stackoverflow.com/questions/57102219/finding-non-overlapping-windows-in-a-pyspark-dataframe

method-1: using spark.udf.registerJavaFunction() to set up Scala UDF in pyspark:

    spark-submit --jars my_udf-assembly-1.0.jar myudf_method_1.py

method-3: 

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, expr, udf, explode

# method-1
def scala_udf_set_group_label(df, N):
    """register the Scala UDF function"""
    spark.udf.registerJavaFunction('set_group_label_1', 'com.jxc.spark.SetGroupLabel2', 'array<struct<t:double,g:int>>')

    """convert t for each id into array and run Spark SQL with the registered Scala UDF function"""
    df_new = df.groupby('id') \
               .agg(collect_list('t').alias('arr')) \
               .withColumn('arr_new', expr('explode(set_group_label_1(arr, {}))'.format(N))) \
               .select('id', 'arr_new.*')
    df_new.show()

# method-3
def pyspark_udf_set_group_label(df, N):
    # function logic in Python
    def set_group_label(arr, N):
        try:
            g = 0; w0 = 0; new_arr = []
            threshold = N + 0.001
            for x in sorted(arr):
                if x - w0 >= threshold:
                    g += 1
                    w0 = x
                new_arr.append({'t':x, 'g':g})
            return new_arr
        except:
            return None

    # set up the pyspark udf:
    set_group_label_3 = udf(lambda x: set_group_label(x, N), 'array<struct<t:double,g:int>>')

    df_new = df.groupby('id') \
               .agg(collect_list('t').alias('arr')) \
               .withColumn('arr_new', explode(set_group_label_3('arr'))) \
               .select('id', 'arr_new.*')
    df_new.show()

if __name__ == "__main__":

    spark = SparkSession.builder.appName("ScalaUDFtoPyspark").getOrCreate()

    d = [ (1,0), (1,1), (1,4), (1,7), (1,14), (1,18)
        , (2,5), (2,20), (2,21)
        , (3,0), (3,1), (3,1.5), (3,2), (3,3.5), (3,4), (3,6)
        , (3,6.5), (3,7), (3,11), (3,14), (3,18), (3,20), (3,24)
        , (4,0), (4,1), (4,2), (4,6), (4,7)
    ]

    mydf = spark.createDataFrame([(int(x[0]), float(x[1])) for x in d], ['id', 't'])

    # threhold seconds to split sub-groups
    N = 6

    # method-1: using Scala-based UDF
    scala_udf_set_group_label(mydf, N)

    # method-3: using pyspark.sql.functions.udf
    pyspark_udf_set_group_label(mydf, N)

    spark.stop()

