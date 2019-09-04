package com.jxc.spark

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.api.java.UDF2
import scala.collection.mutable.ListBuffer

/* case class to handle pyspark StructType
 */
case class TRow(t: Double, g: Integer)

/* UDF function when there is 1 argument
 */
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

/* UDF function when there are 2 arguments, with thredhold added for flexibility
 */
class SetGroupLabel2 extends UDF2[Seq[Double], Integer, Seq[TRow]] {

    override def call(arr: Seq[Double], N: Integer): Seq[TRow] = {
        var g:Int = 0
        var w0:Double = 0
        var new_arr = ListBuffer[TRow]() 
        val threshold:Double = N.toDouble + 0.001
        for (x <- arr.sorted) {
            if (x - w0 >= threshold) {
                g = g+1
                w0 = x
            }
            new_arr += TRow(x, g)
        }
        return new_arr
    }
}
