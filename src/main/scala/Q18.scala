package main.scala

import com.nec.frovedis.sql.FrovedisDataFrame
import com.nec.frovedis.sql.functions.sum
import com.nec.frovedis.sql.implicits_._
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
//import org.apache.spark.sql.functions.first
//import org.apache.spark.sql.functions.sum
//import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 18
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q18 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
    import schemaProvider._

    val time0 = System.nanoTime()

    val fcustomer = new FrovedisDataFrame(customer)
    val flineitem = new FrovedisDataFrame(lineitem)
    val flineitem2 = flineitem.withColumnRenamed("l_orderkey", "key")
    val forder = new FrovedisDataFrame(order)

    val time1 = System.nanoTime()

    val ret = flineitem.groupBy($$"l_orderkey")
      .agg(sum($$"l_quantity").as("sum_quantity"))
      .filter($$"sum_quantity" > 300)
      .select($$"l_orderkey", $$"sum_quantity")
      .join(forder, $$"l_orderkey" === forder("o_orderkey"))
      .join(flineitem2, $$"o_orderkey" === flineitem2("key"))
      .join(fcustomer, $$"o_custkey" === fcustomer("c_custkey"))
      .select($$"l_quantity", $$"c_name", $$"c_custkey", $$"o_orderkey", $$"o_orderdate", $$"o_totalprice")
      .groupBy($$"c_name", $$"c_custkey", $$"o_orderkey", $$"o_orderdate", $$"o_totalprice")
      .agg(sum("l_quantity"))
      .sort($$"o_totalprice".desc, $$"o_orderdate")

    val time2 = System.nanoTime()

    val df = ret.to_spark_DF.limit(100)
    df.collect

    val time3 = System.nanoTime()

    System.out.println("TIMES = " + (time1 - time0) / 1000000 + " " + (time2 - time1) / 1000000 + " " + (time3 - time2) / 1000000)

    df
  }

}
