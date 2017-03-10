package org.tmoerman.brassica.cases.megacell

import _root_.ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import breeze.linalg.{CSCMatrix, SliceMatrix}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{Row, SparkSession}
import org.tmoerman.brassica.ScenicPipeline._
import org.tmoerman.brassica._
import org.tmoerman.brassica.cases.megacell.MegacellReader._

/**
  * @author Thomas Moerman
  */
object MegacellPipeline {

  /**
    * @param spark
    * @param hdf5Path
    * @param parquetPath
    * @param candidateRegulators
    * @param targets
    * @param params
    * @param cellTop
    */
  def apply(spark: SparkSession,
            hdf5Path: String,
            parquetPath: String,
            candidateRegulators: List[Gene],
            targets: List[Gene] = Nil,
            params: RegressionParams = RegressionParams(),
            cellTop: Option[CellCount] = None,
            nrPartitions: Option[Int] = None) = {
    
    val sc = spark.sparkContext

    val allGenes = readGeneNames(hdf5Path).get
    val globalRegulatorIndex = toRegulatorGlobalIndexMap(allGenes, candidateRegulators)

    val csc =
      readCSCMatrix(
        hdf5Path,
        cellTop = cellTop,
        onlyGeneIndices = Some(globalRegulatorIndex.map(_._2))).get

    val cscBroadcast = sc.broadcast(csc)
    val globalRegulatorIndexBroadcast = sc.broadcast(globalRegulatorIndex)

    def isTarget(row: Row) = {
      containedIn(targets)(row.gene)
    }

    def predictRegulators(row: Row) = {
      importanceScores(row.gene, row.data, globalRegulatorIndexBroadcast.value, cscBroadcast.value, params)
    }

    val rdd = spark.read.parquet(parquetPath).rdd

    val GRN =
      nrPartitions.map(rdd.repartition(_)).getOrElse(rdd)
        .filter(isTarget)
        .flatMap(predictRegulators)

    spark
      .createDataFrame(GRN)
      .toDF(CANDIDATE_REGULATOR_NAME, TARGET_GENE_NAME, IMPORTANCE)
  }

  /**
    * @param targetGene The target gene.
    * @param response The response vector of target gene.
    * @param globalRegulatorIndex Global index of the regulator genes.
    * @param csc The CSC matrix of gene expression values, only contains regulators.
    * @param params Parameters for the regressions.
    * @return Calculate the importance scores for the regulators of the target gene.
    */
  def importanceScores(targetGene: String,
                       response: Array[Float],
                       globalRegulatorIndex: List[(Gene, GeneIndex)],
                       csc: CSCMatrix[GeneExpression],
                       params: RegressionParams): Iterable[(Gene, Gene, Importance)] = {

    println(s"-> $targetGene")

    val regulatorCSCIndexTuples =
      globalRegulatorIndex
        .map(_._1)
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene } // remove the target from the predictors

    val predictors = csc.apply(0 until csc.rows, regulatorCSCIndexTuples.map(_._2))

    def toTrainingData = {
      val trainingData = toDMatrix(predictors)
      trainingData.setLabel(response)
      trainingData
    }

    def performXGBoost(trainingData: DMatrix) = {
      import params._

      val booster = XGBoost.train(trainingData, boosterParams, nrRounds)

      val sum = booster.getFeatureScore().values.map(_.toInt).sum

      val scores =
        booster
          .getFeatureScore()
          .map { case (feature, score) => {
            val featureIndex = feature.substring(1).toInt
            val (regulatorGene, _) = regulatorCSCIndexTuples(featureIndex)
            val importance = if (normalize) score.toFloat / sum else score.toFloat

            (regulatorGene, targetGene, importance)}}
          .toSeq

      scores.sortBy(- _._3)
    }

    resource
      .makeManagedResource(toTrainingData)(_.delete)(Nil)
      .map(performXGBoost)
      .opt.get
  }

  private[megacell] def toDMatrix(m: SliceMatrix[Int, Int, Int]) =
    new DMatrix(m.activeValuesIterator.map(_.toFloat).toArray, m.rows, m.cols, 0f)

  private[megacell] implicit class PimpRow(row: Row) {
    def gene: String = row.getAs[String](GENE)
    def data: Array[Float] = row.getAs[SparseVector](VALUES).toArray.map(_.toFloat)
  }

  private[megacell] def containedIn(targets: List[Gene]) = targets match {
    case Nil => (_: Gene) => true
    case _ => targets.toSet.contains _
  }

}