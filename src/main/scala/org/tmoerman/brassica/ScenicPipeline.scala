package org.tmoerman.brassica

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tmoerman.brassica.cases.DataReader

/**
  * @author Thomas Moerman
  */
object ScenicPipeline {

  /**
    * @param spark
    * @param file
    * @param dataReader
    * @param transcriptionFactors
    * @param xgBoostParams
    * @param out
    */
  def apply(spark: SparkSession,
            file: String,
            dataReader: DataReader,
            transcriptionFactors: List[Gene] = Nil,
            xgBoostParams: XGBoostParams,
            out: String): Unit = {

    val (df, genes) = dataReader(spark, file)

    // intersection of the genes in the file and the specified TFs
    val transcriptionFactorIntersection: List[(String, Int)] =
      genes
        .zipWithIndex
        .filter{ case (gene, _) => transcriptionFactors.toSet.contains(gene) }



  }

}