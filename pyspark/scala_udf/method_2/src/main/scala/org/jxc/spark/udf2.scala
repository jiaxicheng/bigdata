package com.jxc.spark

import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ListBuffer

object Functions {

    case class TRow(t: Double, g: Integer)

    def set_group_label_3(arr: Seq[Double], N: Integer): Seq[TRow] = {
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

    def registerFunc(sqlContext: SQLContext, name: String) {
        val f = set_group_label_3(_,_)
        sqlContext.udf.register(name, f)
    }
}
