package main.scala

import java.util.Arrays
import com.nec.frovedis.Jexrpc._
import com.nec.frovedis.matrix._
import com.nec.frovedis.sql._
import com.nec.frovedis.sql.functions._
import com.nec.frovedis.sql.implicits_._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

// TPC-H table schemas
case class Customer(
  c_custkey: Long,
  c_name: String,
  c_address: String,
  c_nationkey: Long,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Long,
  l_partkey: Long,
  l_suppkey: Long,
  l_linenumber: Long,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: Long,
  l_commitdate: Long,
  l_receiptdate: Long,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Long,
  n_name: String,
  n_regionkey: Long,
  n_comment: String)

case class Order(
  o_orderkey: Long,
  o_custkey: Long,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: Long,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Long,
  o_comment: String)

case class Part(
  p_partkey: Long,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Long,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Long,
  ps_suppkey: Long,
  ps_availqty: Long,
  ps_supplycost: Double,
  ps_comment: String)

case class Region(
  r_regionkey: Long,
  r_name: String,
  r_comment: String)

case class Supplier(
  s_suppkey: Long,
  s_name: String,
  s_address: String,
  s_nationkey: Long,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)

class Q06DataFrame extends FrovedisDataFrame {
  def this(df: DataFrame) = {
    this ()
    load (df)
  }

  private def copy_local_data(index: Int, data: Iterator[Row], t_node: Node,
      quantities_vptr: Long, extendedprices_vptr: Long, discounts_vptr: Long, shipdates_vptr: Long) : Iterator[Boolean] = {
    var size: Int = 1024*1024
    var quantities = new Array[Double](size)
    var extendedprices = new Array[Double](size)
    var discounts = new Array[Double](size)
    var shipdates = new Array[Long](size)

    var n: Int = 0
    for (row <- data) {
        if (n >= size) {
            size *= 2
            quantities = Arrays.copyOf(quantities, size)
            extendedprices = Arrays.copyOf(extendedprices, size)
            discounts = Arrays.copyOf(discounts, size)
            shipdates = Arrays.copyOf(shipdates, size)
        }
        quantities(n) = row.getAs[Double](0)
        extendedprices(n) = row.getAs[Double](1)
        discounts(n) = row.getAs[Double](2)
        shipdates(n) = row.getAs[Long](3)
        n+=1
    }
    if (size != n) {
        size = n
        quantities = Arrays.copyOf(quantities, size)
        extendedprices = Arrays.copyOf(extendedprices, size)
        discounts = Arrays.copyOf(discounts, size)
        shipdates = Arrays.copyOf(shipdates, size)
    }
    JNISupport.loadFrovedisWorkerDoubleVector(t_node,quantities_vptr,0,quantities,size)
    JNISupport.loadFrovedisWorkerDoubleVector(t_node,extendedprices_vptr,0,extendedprices,size)
    JNISupport.loadFrovedisWorkerDoubleVector(t_node,discounts_vptr,0,discounts,size)
    JNISupport.loadFrovedisWorkerLongVector(t_node,shipdates_vptr,0,shipdates,size)

    val err = JNISupport.checkServerException()
    if (err != "") throw new java.rmi.ServerException(err)

    Array(true).toIterator
  }

  override def load (df: DataFrame) : Unit = {
    /** releasing the old data (if any) */
    release()
    cols = df.dtypes.map(_._1)
    val tt = df.dtypes.map(_._2)
    val size = cols.size
    require(size == 4)
    var dvecs = new Array[Long](size)
    types = new Array[Short](size)
    for (i <- 0 to (size-1)) {
      //print("col_name: " + cols(i) + " col_type: " + tt(i) + "\n")
      val tname = tt(i)
      types(i) = TMAPPER.string2id(tname)
//      dvecs(i) = TMAPPER.toTypedDvector(df,tname,i)
    }

    val fs = FrovedisServer.getServerInstance()
    val nproc = fs.worker_size
    val npart = df.rdd.getNumPartitions
    require(nproc == npart)
    val block_sizes = GenericUtils.get_block_sizes(npart, nproc)

    val quantities_vptrs = JNISupport.allocateLocalVector(fs.master_node, block_sizes, nproc, DTYPE.DOUBLE)
    val extendedprices_vptrs = JNISupport.allocateLocalVector(fs.master_node, block_sizes, nproc, DTYPE.DOUBLE)
    val discounts_vptrs = JNISupport.allocateLocalVector(fs.master_node, block_sizes, nproc, DTYPE.DOUBLE)
    val shipdates_vptrs = JNISupport.allocateLocalVector(fs.master_node, block_sizes, nproc, DTYPE.LONG)
    val fw_nodes = JNISupport.getWorkerInfoMulti(fs.master_node, block_sizes.map(_ * size), nproc)
    var info = JNISupport.checkServerException()
    if (info != "") throw new java.rmi.ServerException(info)
    val ret = df.rdd.mapPartitionsWithIndex(
      (i,x) => copy_local_data(i,x,fw_nodes(i),quantities_vptrs(i),extendedprices_vptrs(i),discounts_vptrs(i),shipdates_vptrs(i)))
    ret.count

    dvecs(0) = JNISupport.createFrovedisDvector(fs.master_node,quantities_vptrs,nproc,DTYPE.DOUBLE)
    dvecs(1) = JNISupport.createFrovedisDvector(fs.master_node,extendedprices_vptrs,nproc,DTYPE.DOUBLE)
    dvecs(2) = JNISupport.createFrovedisDvector(fs.master_node,discounts_vptrs,nproc,DTYPE.DOUBLE)
    dvecs(3) = JNISupport.createFrovedisDvector(fs.master_node,shipdates_vptrs,nproc,DTYPE.LONG)

    fdata = JNISupport.createFrovedisDataframe(fs.master_node,types,cols,dvecs,size)
    info = JNISupport.checkServerException()
    if (info != "") throw new java.rmi.ServerException(info)
  }
}

class TpchSchemaProvider(sc: SparkContext, inputDir: String) {

  // this is used to implicitly convert an RDD to a DataFrame.
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  val dfMapRaw = Map(
    "customer" -> sc.textFile(inputDir + "/customer.tbl*").map(_.split('|')).map(p =>
      Customer(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),

    "lineitem" -> sc.textFile(inputDir + "/lineitem.tbl*").map(_.split('|')).map(p =>
      Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, TpchQuery.parseDate(p(10).trim), TpchQuery.parseDate(p(11).trim), TpchQuery.parseDate(p(12).trim), p(13).trim, p(14).trim, p(15).trim)).toDF(),

    "nation" -> sc.textFile(inputDir + "/nation.tbl*").map(_.split('|')).map(p =>
      Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim)).toDF(),

    "region" -> sc.textFile(inputDir + "/region.tbl*").map(_.split('|')).map(p =>
      Region(p(0).trim.toLong, p(1).trim, p(2).trim)).toDF(),

    "order" -> sc.textFile(inputDir + "/orders.tbl*").map(_.split('|')).map(p =>
      Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, TpchQuery.parseDate(p(4).trim), p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF(),

    "part" -> sc.textFile(inputDir + "/part.tbl*").map(_.split('|')).map(p =>
      Part(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toLong, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF(),

    "partsupp" -> sc.textFile(inputDir + "/partsupp.tbl*").map(_.split('|')).map(p =>
      Partsupp(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toDouble, p(4).trim)).toDF(),

    "supplier" -> sc.textFile(inputDir + "/supplier.tbl*").map(_.split('|')).map(p =>
      Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF())

  val dfMap = dfMapRaw.map { case(key, value) => (key, value.repartition(8).cache) }

  // for implicits
  val customer = dfMap.get("customer").get
  val lineitem = dfMap.get("lineitem").get
  val nation = dfMap.get("nation").get
  val region = dfMap.get("region").get
  val order = dfMap.get("order").get
  val part = dfMap.get("part").get
  val partsupp = dfMap.get("partsupp").get
  val supplier = dfMap.get("supplier").get

  dfMap.foreach {
    case (key, value) => value.count; value.createOrReplaceTempView(key)
  }
}
