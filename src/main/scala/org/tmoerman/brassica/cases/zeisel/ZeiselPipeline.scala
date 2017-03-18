package org.tmoerman.brassica.cases.zeisel

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tmoerman.brassica._

/**
  * @author Thomas Moerman
  */
object ZeiselPipeline {

  def apply(spark: SparkSession,
            raw: Path,
            candidateRegulators: Set[Gene],
            targets: Set[Gene] = Set.empty,
            params: RegressionParams = RegressionParams(),
            cellTop: Option[CellCount] = None,
            nrPartitions: Option[Int] = None): DataFrame = {

    val lines = ZeiselReader.rawLines(spark, raw)

    val cscProducer = (globalRegulatorIndex: List[(Gene, GeneIndex)]) =>
      ZeiselReader
        .readCSCMatrix(
          lines,
          onlyGeneIndices = globalRegulatorIndex.map(_._2))

    val expressionByGene = ZeiselReader.readExpressionByGene(spark, lines)

    val allGenes = ZeiselReader.readGenes(lines)

    ScenicPipeline
      .apply(
        spark,
        cscProducer,
        expressionByGene,
        allGenes,
        candidateRegulators,
        targets,
        params,
        cellTop,
        nrPartitions)
  }

  def crossValidate(spark: SparkSession,
                    raw: Path,
                    candidateRegulators: Set[Gene],
                    targets: Set[Gene],
                    params: RegressionParams = RegressionParams(),
                    cellTop: Option[CellCount] = None,
                    nrPartitions: Option[Int]) = {
    // TODO
  }

}