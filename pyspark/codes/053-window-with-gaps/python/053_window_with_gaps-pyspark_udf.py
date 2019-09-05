"""
https://stackoverflow.com/questions/57186107/pyspark-iterate-over-groups-in-a-dataframe

The data are grouped by id and sorted by Updated_date, for the stateful problem where
the calculation on the current row are influenced by the updated values of the previous
rows, the vectorized functions/methods are often not working. UDF is the last resort for 
any such difficulty problems.

Objective: create two new columns LB and UB in such a way that: for each id
           , the first values of LB and UB are the values of an interval of (date +/- 10 days)
           , for the next values having the same id, we verify if the date is between LB and UB 
           of the previous row, if yes we use the same values, if not we recompute a new 
           interval of (+/- 10 days).

>>> df_new.show()
+---+-------------------+----------+----------+----------+
| id|       Updated_date|      date|        LB|        UB|
+---+-------------------+----------+----------+----------+
|  b|2018-11-15 10:36:59|2019-01-25|2019-01-15|2019-02-04|
|  b|2018-11-16 10:58:01|2019-02-10|2019-01-31|2019-02-20|
|  b|2018-11-17 10:42:12|2019-02-04|2019-01-31|2019-02-20|
|  b|2018-11-24 10:24:56|2019-02-10|2019-01-31|2019-02-20|
|  b|2018-12-01 10:28:46|2019-02-02|2019-01-31|2019-02-20|
|  a|2018-10-30 10:25:45|2019-02-14|2019-02-04|2019-02-24|
|  a|2018-11-28 10:51:34|2019-02-14|2019-02-04|2019-02-24|
|  a|2018-11-29 10:46:07|2019-01-11|2019-01-01|2019-01-21|
|  a|2018-11-30 10:42:56|2019-01-14|2019-01-01|2019-01-21|
|  a|2018-12-01 10:28:46|2019-01-16|2019-01-01|2019-01-21|
|  a|2018-12-02 10:22:06|2019-01-22|2019-01-12|2019-02-01|
+---+-------------------+----------+----------+----------+

Run the program:

    spark-submit 053_udf-window_with_gaps.py

"""
from pyspark.sql import SparkSession, Window, functions as F
from datetime import timedelta

def pyspark_udf_set_group_label(df, N, cols):

    def set_subgroup(arr, N):
        lb = None; ub = None; new_arr = []
        delta_dt = timedelta(days=N)
        for x in sorted(arr, key=lambda x: x.Updated_date):
            """x here is an Row() object"""
            if lb is None or x.date > ub or x.date < lb:
                lb = x.date - delta_dt
                ub = x.date + delta_dt
            """below lines merge two dicts used for Python 2.7
               for Python-3, change the below 3 lines to one line
                   new_arr.append({**x.asDict(), **{'LB': lb, 'UB': ub}})
            """
            mydict = {'LB': lb, 'UB': ub}
            mydict.update(x.asDict())
            new_arr.append(mydict)
        return new_arr

    schema = 'array<struct<Updated_date:timestamp,date:date,LB:date,UB:date>>'

    udf_set_subgroup = F.udf(lambda x: set_subgroup(x, N), schema)

    df.groupby('id')                                           \
      .agg(F.collect_list(F.struct(cols)).alias('data'))       \
      .withColumn('arr', F.explode(udf_set_subgroup('data')))  \
      .select('id', 'arr.*')                                   \
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

    # columns to included in the final result, the schema must matches these columns
    cols = [ c for c in mydf.columns if c not in ['id'] ]

    # convert columne to the proper data types
    mydf = mydf.withColumn('date', F.to_date('date')).withColumn('Updated_date', F.to_timestamp('Updated_date'))

    # call the pyspark udf-function to set up the label
    pyspark_udf_set_group_label(mydf, N, cols)

    spark.stop()

