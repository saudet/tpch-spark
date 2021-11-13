package main.scala

import com.nec.frovedis.sql.FrovedisDataFrame
import com.nec.frovedis.sql.functions.max
import com.nec.frovedis.sql.functions.sum
import com.nec.frovedis.sql.implicits_._
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
//import org.apache.spark.sql.functions.max
//import org.apache.spark.sql.functions.sum
//import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 15
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q15 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
    import schemaProvider._

    val time0 = System.nanoTime()

    val flineitem = new FrovedisDataFrame(lineitem)
    val fsupplier = new FrovedisDataFrame(supplier)

//    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val time1 = System.nanoTime()

    val revenue = flineitem.filter($$"l_shipdate" >= TpchQuery.parseDate("1996-01-01") &&
      $$"l_shipdate" < TpchQuery.parseDate("1996-04-01"))
      .select($$"l_suppkey", $$"l_extendedprice")
      .groupBy($$"l_suppkey")
      .agg(sum($$"l_extendedprice").as("total"))
    // .cache

    val ret = new FrovedisDataFrame(revenue.agg(max($$"total").as("max_total")))
      .join(revenue, $$"max_total" === revenue("total"))
      .join(fsupplier, $$"l_suppkey" === fsupplier("s_suppkey"))
      .select($$"s_suppkey", $$"s_name", $$"s_address", $$"s_phone", $$"total")
      .sort($$"s_suppkey")

    val time2 = System.nanoTime()

    val df = ret.to_spark_DF

    val time3 = System.nanoTime()

    System.out.println("TIMES = " + (time1 - time0) / 1000000 + " " + (time2 - time1) / 1000000 + " " + (time3 - time2) / 1000000)

    df
  }

}
