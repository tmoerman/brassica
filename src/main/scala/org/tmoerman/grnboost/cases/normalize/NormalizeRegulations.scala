package org.tmoerman.grnboost.cases.normalize

import org.apache.spark.sql.SparkSession
import org.tmoerman.grnboost.GRN_BOOST
import org.tmoerman.grnboost.cases.DataReader.readRegulation

/**
  * @author Thomas Moerman
  */
object NormalizeRegulations {

  def main(args: Array[String]): Unit = {
    val in  = args(0)
    val out = args(1)

    val spark =
      SparkSession
        .builder
        .appName(s"$GRN_BOOST - normalize regulations")
        .getOrCreate
    
    import org.apache.spark.sql.functions._

    readRegulation(spark, in)
      .normalizeBy(avg)
      .saveTxt(out)
  }

}