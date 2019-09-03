package com.jxc.spark

import org.apache.spark.sql.api.java.UDF1
import scala.collection.mutable.ListBuffer


/*
  caseclass: https://stackoverflow.com/questions/45080913/using-spark-udfs-with-struct-sequences
             https://docs.scala-lang.org/tour/case-classes.html
  spark-sql-udf: https://stackoverflow.com/questions/38413040/spark-sql-udf-with-complex-input-parameter
  difference between List and Array: https://stackoverflow.com/questions/2712877/difference-between-array-and-list-in-scala

 */

case class TRow(t: Double, g: Integer)

class SetGroupLabel extends UDF1[Seq[Double], Seq[TRow]] {

    override def call(arr: Seq[Double]): Seq[TRow] = {
        var g:Int = 0
        var w0:Double = 0
        var new_arr = ListBuffer[TRow]() 
        for (x <- arr.sorted) {
            if (x - w0 >= 5.0001) {
                g = g+1
                w0 = x
            }
            new_arr += TRow(x, g)
        }
        return new_arr
    }
}
