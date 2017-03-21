package org.tmoerman.brassica.cases.zeisel

import org.apache.spark.sql.SparkSession
import org.tmoerman.brassica._

/**
  * @author Thomas Moerman
  */
object ZeiselFilteredPipeline {

  def apply(spark: SparkSession,
            file: Path,
            candidateRegulators: Set[Gene],
            targets: Set[Gene],
            params: RegressionParams = RegressionParams(),
            nrPartitions: Option[Int] = None) = {

    val expressionByGene = ZeiselFilteredReader.apply(spark, file)

    val allGenes = ZeiselFilteredReader.toGenes(spark, expressionByGene)

    ScenicPipeline
      .apply(
        spark,
        expressionByGene,
        allGenes,
        candidateRegulators,
        targets,
        params,
        nrPartitions)
  }

}