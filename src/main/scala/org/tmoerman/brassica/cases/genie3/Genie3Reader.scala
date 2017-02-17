package org.tmoerman.brassica.cases.genie3

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tmoerman.brassica.cases.DataReader

import org.tmoerman.brassica._

/**
  * @author Thomas Moerman
  */
object Genie3Reader extends DataReader {

  /**
    * @param spark The SparkSession.
    * @param files The Genie3 expression file name.
    * @return Returns a tuple:
    *         - DataFrame
    *         - Gene list
    */
  def apply(spark: SparkSession, files: String*): (DataFrame, List[String]) = {
    val csv =
      spark
        .read
        .option("header", true)
        .option("inferSchema", true)
        .option("delimiter", "\t")
        .csv(files.head)

    val assembler =
      new VectorAssembler()
        .setInputCols(csv.columns)
        .setOutputCol(EXPRESSION_VECTOR)

    val df = assembler.transform(csv).select(EXPRESSION_VECTOR)

    (df, csv.columns.toList)
  }

}