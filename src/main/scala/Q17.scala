package main.scala

import com.nec.frovedis.sql.FrovedisDataFrame
import com.nec.frovedis.sql.functions.avg
import com.nec.frovedis.sql.functions.sum
import com.nec.frovedis.sql.implicits_._
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
//import org.apache.spark.sql.functions.avg
//import org.apache.spark.sql.functions.sum
//import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 17
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q17 extends TpchQuery {

  override def execute(sc: SparkContext, schemaProvider: TpchSchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
    import schemaProvider._

    val time0 = System.nanoTime()

    val flineitem = new FrovedisDataFrame(lineitem, "l_partkey", "l_quantity", "l_extendedprice")
    val fpart = new FrovedisDataFrame(part, "p_partkey", "p_brand", "p_container")

//    val mul02 = udf { (x: Double) => x * 0.2 }

    val time1 = System.nanoTime()

    val fpart2 = fpart.filter($$"p_brand" === "Brand#23" && $$"p_container" === "MED BOX")
      .select($$"p_partkey")
      .join(flineitem, $$"p_partkey" === flineitem("l_partkey"), "left_outer")
    val fpart3 = fpart2.withColumnRenamed("p_partkey", "key")
    // select

    val ret = fpart2.groupBy("p_partkey")
      .agg(avg($$"l_quantity").as("avg_quantity"))
      .select($$"p_partkey", $$"avg_quantity")
      .join(fpart3, $$"p_partkey" === fpart3("key"))
      .filter($$"l_quantity" < $$"avg_quantity")
      .agg(sum($$"l_extendedprice"))

    val time2 = System.nanoTime()

    val df = ret.to_spark_DF
    df.collect

    val time3 = System.nanoTime()

    System.out.println("TIMES = " + (time1 - time0) / 1000000 + " " + (time2 - time1) / 1000000 + " " + (time3 - time2) / 1000000)

    df
  }

}
