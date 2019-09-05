from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, expr, struct, col, from_unixtime, collect_list

def scala_udf_set_group_label(df, N):

    #cols = [ c for c in df.columns if c not in ['id'] ]
    cols = ['Updated_date', 'date']
    cols1 = ['Updated_date', 'date', 'LB', 'UB']

    N = N*86400

    schema = 'array<struct<{}>>'.format(','.join(['{}:Long'.format(c) for c in cols1]))

    spark.udf.registerJavaFunction('set_date_bound_1', 'com.jxc.spark.SetWindowBoundary', schema)

    df.withColumn('date', unix_timestamp('date', format='yyyy-MM-dd')) \
      .withColumn('Updated_date', unix_timestamp('Updated_date'))      \
      .groupby('id')                                                     \
      .agg(collect_list(struct(cols)).alias('data'))                 \
      .withColumn('arr', expr('explode(set_date_bound_1(data, {}))'.format(N)))  \
      .select('id'
            , from_unixtime(col('arr')['Updated_date']).alias('Updated_date')
            , *[ from_unixtime(col('arr')[c], format="yyyy-MM-dd").alias(c) for c in ['date', 'LB', 'UB'] ]
       ).show()

if __name__ == "__main__":

    spark = SparkSession.builder                           \
                        .master('local[*]')                \
                        .appName('UDF-Moving-GapWindow')   \
                        .getOrCreate()

    """ Example data """
    mydf = spark.createDataFrame([
              ('a', '2019-02-14', '2018-10-30 10:25:45')
            , ('a', '2019-02-14', '2018-11-28 10:51:34')
            , ('a', '2019-01-11', '2018-11-29 10:46:07')
            , ('a', '2019-01-14', '2018-11-30 10:42:56')
            , ('a', '2019-01-16', '2018-12-01 10:28:46')
            , ('a', '2019-01-22', '2018-12-02 10:22:06')
            , ('b', '2019-01-25', '2018-11-15 10:36:59')
            , ('b', '2019-02-10', '2018-11-16 10:58:01')
            , ('b', '2019-02-04', '2018-11-17 10:42:12')
            , ('b', '2019-02-10', '2018-11-24 10:24:56')
            , ('b', '2019-02-02', '2018-12-01 10:28:46')
        ], ['id', 'date', 'Updated_date']
    )

    # number of second before and after the current Row to define the Window-size
    N = 10

    # call the pyspark udf-function to set up the label
    scala_udf_date_set_group_label(mydf, N)

    spark.stop()

