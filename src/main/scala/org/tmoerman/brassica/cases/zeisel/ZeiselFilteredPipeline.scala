package org.tmoerman.brassica.cases.zeisel

import org.apache.spark.sql.SparkSession
import org.tmoerman.brassica._
import org.tmoerman.brassica.cases.zeisel.ZeiselReader.toCSCMatrix

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

    val cscProducer = (regulators: List[Gene]) => toCSCMatrix(expressionByGene, regulators)

    val allGenes = ZeiselFilteredReader.toGenes(spark, expressionByGene)

    ScenicPipeline
      .apply(
        spark,
        cscProducer,
        expressionByGene,
        allGenes,
        candidateRegulators,
        targets,
        params,
        cellTop = None,
        nrPartitions)
  }

}