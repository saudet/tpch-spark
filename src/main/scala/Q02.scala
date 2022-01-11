package main.scala

import com.nec.frovedis.sql.FrovedisDataFrame
import com.nec.frovedis.sql.functions.min
import com.nec.frovedis.sql.implicits_._
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
//import org.apache.spark.sql.functions.min

/**
 * TPC-H Query 2
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q02 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
    import schemaProvider._

    val time0 = System.nanoTime()

//    val flineitem = new FrovedisDataFrame(lineitem)
//    val forder = new FrovedisDataFrame(order)
    val fregion = new FrovedisDataFrame(region, "r_regionkey", "r_name")
    val fnation  = new FrovedisDataFrame(nation, "n_nationkey", "n_regionkey", "n_name")
    val fsupplier = new FrovedisDataFrame(supplier)
    val fpartsupp = new FrovedisDataFrame(partsupp, "ps_partkey", "ps_suppkey", "ps_supplycost")
    val fpart = new FrovedisDataFrame(part, "p_partkey", "p_size", "p_type", "p_mfgr")

    val time1 = System.nanoTime()

    val europe = fregion.filter($$"r_name" === "EUROPE")
      .join(fnation, $$"r_regionkey" === fnation("n_regionkey"))
      .join(fsupplier, $$"n_nationkey" === fsupplier("s_nationkey"))
      .join(fpartsupp, fsupplier("s_suppkey") === fpartsupp("ps_suppkey"))
      .select($$"ps_partkey", $$"ps_supplycost", $$"s_acctbal", $$"s_name", $$"n_name", $$"s_address", $$"s_phone", $$"s_comment")

    europe.describe().show()
    val brass = fpart.filter(fpart("p_size") === 15 && fpart("p_type").like("%BRASS"))
      .join(europe, $$"p_partkey" === europe("ps_partkey"))
    //.cache

    val minCost = brass.groupBy(brass("ps_partkey"))
      .agg(min("ps_supplycost").as("min"))

    val ret = brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($$"s_acctbal".desc, $$"n_name", $$"s_name", $$"p_partkey")

    val time2 = System.nanoTime()

    val df = ret.to_spark_DF.limit(100)
    df.collect

    val time3 = System.nanoTime()

    System.out.println("TIMES = " + (time1 - time0) / 1000000 + " " + (time2 - time1) / 1000000 + " " + (time3 - time2) / 1000000)

    df
  }

}
