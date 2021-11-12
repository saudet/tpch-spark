package main.scala

import com.nec.frovedis.sql.FrovedisDataFrame
import com.nec.frovedis.sql.functions.sum
import com.nec.frovedis.sql.implicits_._
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
//import org.apache.spark.sql.functions.sum
//import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 6
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q06 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
    import schemaProvider._

    val time0 = System.nanoTime()

    val flineitem = new FrovedisDataFrame(lineitem)

    val time1 = System.nanoTime()

    val ret = flineitem.filter($$"l_shipdate" >= TpchQuery.parseDate("1994-01-01") && $$"l_shipdate" < TpchQuery.parseDate("1995-01-01") && $$"l_discount" >= 0.05 && $$"l_discount" <= 0.07 && $$"l_quantity" < 24)
      .agg(sum($$"l_extendedprice"), sum($$"l_discount"))

    val time2 = System.nanoTime()

    ret.count

    val time3 = System.nanoTime()

    System.out.println("TIMES = " + (time1 - time0) / 1000000 + " " + (time2 - time1) / 1000000 + " " + (time3 - time2) / 1000000)

    ret
  }

}
