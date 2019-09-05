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

Tasks:

(1) Use pyspark.sql.functions.udf()
    python/053_window_with_gaps-pyspark_udf.py

(2) Use scala-based udf function and pass the Date and Timestamp
    fields using data type `Long`
    python/053_window_with_gaps-scala_udf_with_long.py

(3) Use scala-based udf function and pass the Date and Timestamp
    to match Scala: java.sql.{Date,Timestamp}
    python/053_window_with_gaps-scala_udf_with_long.py

