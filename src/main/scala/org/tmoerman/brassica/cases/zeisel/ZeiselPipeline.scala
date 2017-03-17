package org.tmoerman.brassica.cases.zeisel

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tmoerman.brassica._

/**
  * @author Thomas Moerman
  */
object ZeiselPipeline {

  def fromParquet(spark: SparkSession,
                  parquet: Path,
                  raw: Path,
                  candidateRegulators: Set[Gene],
                  targets: Set[Gene],
                  params: RegressionParams = RegressionParams(),
                  cellTop: Option[CellCount] = None,
                  nrPartitions: Option[Int] = None): DataFrame = {

    val df = spark.read.parquet(parquet)

    val cscProducer = (globalRegulatorIndex: List[(Gene, GeneIndex)]) =>
      ZeiselReader
        .toCSCMatrix(
          df,
          cellTop,
          globalRegulatorIndex.map(_._2))

    val allGenes = ZeiselReader.readGenes(spark, raw)

    // TODO parquet for expression By Gene ~ gene columns.
    val lines = ZeiselReader.rawLines(spark, raw)
    val expressionByGene = ZeiselReader.readExpressionByGene(spark, lines)

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

  def apply(spark: SparkSession,
            raw: Path,
            candidateRegulators: Set[Gene],
            targets: Set[Gene] = Set.empty,
            params: RegressionParams = RegressionParams(),
            cellTop: Option[CellCount] = None,
            nrPartitions: Option[Int] = None): DataFrame = {

    val lines = ZeiselReader.rawLines(spark, raw).cache

    // TODO not a list of tuples as argument
    val cscProducer = (globalRegulatorIndex: List[(Gene, GeneIndex)]) =>
      ZeiselReader
        .readCSCMatrix(
          lines,
          onlyGeneIndices = Some(globalRegulatorIndex.map(_._2)))

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

}