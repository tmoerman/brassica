package org.tmoerman.grnboost.util

import org.apache.spark.sql.SparkSession
import org.tmoerman.grnboost._

/**
  * @author Thomas Moerman
  */
object ElbowAdder {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder
        .appName(GRN_BOOST)
        .getOrCreate

    import spark.implicits._

    val in  = args(0)
    val out = args(0)

    spark
      .sparkContext
      .textFile(in)
      .map(_.split("\t"))
      .map{ case Array(r, t, g) => RawRegulation(r, t, g.toFloat) }
      .toDS
      .addElbowGroups(XGBoostRegressionParams())
      .rdd
      .map(_.productIterator.mkString("\t"))
      .repartition(1)
      .saveAsTextFile(out)
  }

}