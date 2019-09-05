"""
Use datetime-based scala UDF to calculate LB/UB of a moving Window containing gaps.
in Scala classes: java.sql.{Date,Timestamp} and java.util.Calendar

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp, struct, expr, collect_list

def scala_udf_date_set_group_label(df, N):

    schema = 'array<struct<Updated_date:Timestamp,date:date,LB:date,UB:date>>'

    spark.udf.registerJavaFunction('set_date_bound_2', 'com.jxc.spark.SetWindowBoundary2', schema)

    df.withColumn('date', to_date('date'))                                      \
      .withColumn('Updated_date', to_timestamp('Updated_date'))                 \
      .groupby('id')                                                            \
      .agg(collect_list(struct('Updated_date', 'date')).alias('data'))          \
      .withColumn('arr', expr('explode(set_date_bound_2(data, {}))'.format(N))) \
      .select('id', 'arr.*')                                                    \
      .show()

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

