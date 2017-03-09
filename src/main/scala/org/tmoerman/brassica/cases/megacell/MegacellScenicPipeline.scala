package org.tmoerman.brassica.cases.megacell

import _root_.ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import breeze.linalg.{CSCMatrix, DenseMatrix}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{Row, SparkSession}
import org.tmoerman.brassica.ScenicPipeline._
import org.tmoerman.brassica._
import org.tmoerman.brassica.cases.megacell.MegacellReader._

import scala.collection.immutable.ListMap

/**
  * @author Thomas Moerman
  */
object MegacellScenicPipeline {

  def apply(spark: SparkSession,
            hdf5Path: String,
            parquetPath: String,
            candidateRegulators: List[Gene],
            targets: List[Gene] = Nil,
            params: RegressionParams = RegressionParams(),
            normalize: Boolean = true): Unit = {

    val sc = spark.sparkContext

    val allGenes = readGeneNames(hdf5Path).get
    val regulatorGlobalIndexMap = toRegulatorGlobalIndexMap(allGenes, candidateRegulators)

    val csc = readCSCMatrix(hdf5Path, onlyGeneIndices = Some(regulatorGlobalIndexMap.values.toSeq)).get

    val cscBroadcast   = sc.broadcast(csc)
    val indexBroadcast = sc.broadcast(regulatorGlobalIndexMap)

    val isTarget = toPredicate(targets)

    val GRN =
      spark
        .read
        .parquet(parquetPath)
        .rdd
        .filter(row => isTarget(row.gene))
        .flatMap(row => importanceScores(row.gene, row.data, indexBroadcast.value, cscBroadcast.value, params))

    spark
      .createDataFrame(GRN)
      .toDF(CANDIDATE_REGULATOR_NAME, TARGET_GENE_NAME, IMPORTANCE)
      .sort(desc(IMPORTANCE))
  }

  private[megacell] implicit class PimpRow(row: Row) {
    def gene: String = row.getAs[String](GENE)
    def data: Array[Float] = row.getAs[SparseVector](VALUES).toArray.map(_.toFloat)
  }

  private[megacell] def importanceScores(targetGene: String,
                                     responseVector: Array[Float],
                                     index: ListMap[Gene, GeneIndex],
                                     csc: CSCMatrix[GeneExpression],
                                     params: RegressionParams) = {
    import params._

    // prepare training data
    val cleanCSCIndexTuples =
      index
        .keys
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene }
        .toSeq

    // compile training data
    val predictorCSC = csc.apply(0 until csc.rows, cleanCSCIndexTuples.map(_._2)).toDenseMatrix
    val trainingData = toDMatrix(predictorCSC)
    trainingData.setLabel(responseVector)

    // train the model
    val booster = XGBoost.train(trainingData, boosterParams, nrRounds)

    // extract the importance scores.
    val sum = booster.getFeatureScore().values.map(_.toInt).sum
    val importanceScores =
      booster
        .getFeatureScore()
        .map { case (feature, score) => {
          val featureIndex = feature.substring(1).toInt
          val (regulatorGene, _) = cleanCSCIndexTuples(featureIndex)
          val importance = if (normalize) score.toFloat / sum else score.toFloat

          (regulatorGene, targetGene, importance)
        }}

    importanceScores
  }

  private[megacell] def toPredicate(targets: List[Gene]) = targets match {
    case Nil => (_: Gene) => true
    case _ => targets.toSet.contains _
  }

  private[megacell] def toDMatrix(dm: DenseMatrix[GeneExpression]) =
    new DMatrix(dm.data.map(_.toFloat), dm.rows, dm.cols, 0f)

}