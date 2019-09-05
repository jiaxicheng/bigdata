package com.jxc.spark

import org.apache.spark.sql.api.java.UDF2
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Row

/* case class to handle pyspark StructType
 * converted the Date and Timestamp fields into Long
 * Notes: case class can not be used in function/method arguments
 *        use org.apache.spark.sql.Row instead, for more details, check
 *        http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Row
 */
case class ORow(Updated_date: Long, date: Long, LB: Long, UB: Long)

/* UDF function when there is 2 argument
 */
class SetWindowBoundary extends UDF2[Seq[Row], Int, Seq[ORow]] {

    override def call(arr: Seq[Row], N: Int): Seq[ORow] = {
        var lb:Long = 0
        var ub:Long = 0
        var new_arr = ListBuffer[ORow]()
        val delta_dt:Long = N.toLong
        for (x <- arr.sortWith(_.getAs[Long]("Updated_date") < _.getAs[Long]("Updated_date"))) {
            var x_date:Long = x.getAs[Long]("date")
            if ((lb == 0) || (x_date > ub) || (x_date < lb)) {
                lb = x_date - delta_dt
                ub = x_date + delta_dt
            }
            new_arr += ORow(x.getAs[Long]("Updated_date"), x_date, lb, ub)
        }
        return new_arr
    }
}

