package org.tmoerman.brassica.cases.megacell

import breeze.linalg.{Matrix, SparseVector, _}
import org.apache.spark.sql.SparkSession
import org.tmoerman.brassica.ScenicPipeline._
import org.tmoerman.brassica.cases.megacell.MegacellReader._
import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import org.tmoerman.brassica.{ExpressionValue, Gene, XGBoostParams}

/**
  * @author Thomas Moerman
  */
object MegacellScenicPipeline {

  def apply(spark: SparkSession,
            hdf5Path: String,
            parquetPath: String,
            candidateRegulators: List[Gene],
            targets: List[Gene] = Nil,
            params: XGBoostParams = XGBoostParams(),
            normalize: Boolean = true): Unit = {

    val sc = spark.sparkContext

    val allGenes = readGeneNames(hdf5Path).get
    val regulatorGlobalIndexMap = toRegulatorGlobalIndexMap(allGenes, candidateRegulators)

    val csc = readCSCMatrix(hdf5Path, onlyGeneIndices = Some(regulatorGlobalIndexMap.values.toSeq)).get

    val cscBroadcast   = sc.broadcast(csc)
    val indexBroadcast = sc.broadcast(regulatorGlobalIndexMap)

    val isTarget = toPredicate(targets)

    val grn =
      spark
        .read
        .parquet(parquetPath)
        .rdd
        .filter(row => {
          val rowGene: Gene = ???
          isTarget(rowGene) })
        .flatMap(row => {
          val csc = cscBroadcast.value

          val targetGene: Gene = ???
          val responseVector: SparseVector[ExpressionValue] = ???

          val cleanCSCIndexTuples =
            indexBroadcast
              .value.keys.zipWithIndex
              .filterNot{ case (gene, _) => gene == targetGene }
              .toSeq

          // prep training data
          val predictorCSC = csc.apply(0 until csc.rows, cleanCSCIndexTuples.map(_._2)).toDenseMatrix
          val trainingData = toDMatrix(predictorCSC)
          val y = responseVector.toDenseVector.data.map(_.toFloat)
          trainingData.setLabel(y)

          // train the model
          val booster =
            XGBoost
              .train(
                trainingData,
                params.boosterParams,
                params.nrRounds)

          // extract the importance scores.
          val sum = booster.getFeatureScore().values.map(_.toInt).sum

          booster
            .getFeatureScore()
            .map{ case (feature, score) => {
              val featureIndex = feature.substring(1).toInt
              val (regulatorGene, _) = cleanCSCIndexTuples(featureIndex)
              val importance = if (normalize) score.toFloat / sum else score.toFloat

              (regulatorGene, targetGene, importance)
            }}})
  }

  private[this] def toPredicate(targets: List[Gene]) = targets match {
    case Nil => (_: Gene) => true
    case _ => targets.toSet.contains _
  }

  def toDMatrix(dm: DenseMatrix[Int]) = new DMatrix(dm.data.map(_.toFloat), dm.rows, dm.cols, 0f)

}