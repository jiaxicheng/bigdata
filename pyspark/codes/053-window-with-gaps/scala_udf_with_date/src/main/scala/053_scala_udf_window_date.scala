package com.jxc.spark

import org.apache.spark.sql.api.java.UDF2
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Row
import java.sql.{Date,Timestamp}
import java.util.Calendar

/* case class and Row to handle pyspark StructType
 * use the java.sql.Date and java.sql.Timestamp
 * Notes: case class can not be used in function/method arguments
 *        use org.apache.spark.sql.Row instead, for more details, check
 *        http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Row
 */
case class ORow(Updated_date: Timestamp, date: Date, LB: Date, UB: Date)

/* 
 * UDF function when there is 2 argument
 */
class SetWindowBoundary2 extends UDF2[Seq[Row], Int, Seq[ORow]] {

    override def call(arr: Seq[Row], delta_dt: Int): Seq[ORow] = {
        var lb:Date = null
        var ub:Date = null
        var new_arr = ListBuffer[ORow]()
        var cal = Calendar.getInstance()
        for (x <- arr.sortWith((e1, e2) => sortByUpdated(e1, e2))) {
            var x_date:Date = x.getAs[Date]("date")
            if (Option(lb).isEmpty || x_date.after(ub) || x_date.before(lb)) {
                cal.setTime(x_date)
                /* set up the upper boundary */
                cal.add(Calendar.DATE, delta_dt)
                ub = new Date(cal.getTimeInMillis())
                /* set up the lower boundary */
                cal.add(Calendar.DATE, -2*delta_dt)
                lb = new Date(cal.getTimeInMillis())
            }
            new_arr += ORow(x.getAs[Timestamp]("Updated_date"), x_date, lb, ub)
        }
        return new_arr
    }

    /*
     * function to sort a Sequence of Rows using a Timestamp field.
     */
    def sortByUpdated(e1: Row, e2: Row): Boolean = {
        e1.getAs[Timestamp]("Updated_date").before(e2.getAs[Timestamp]("Updated_date"))
    }

}

