package org.tmoerman.brassica.algo

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.java.Booster
import ml.dmlc.xgboost4j.java.JXGBoostAccess.createBooster
import ml.dmlc.xgboost4j.scala.DMatrix
import ml.dmlc.xgboost4j.scala.XGBoostAccess.inner
import org.apache.commons.lang.StringUtils.EMPTY
import org.apache.spark.ml.linalg.Vectors.dense
import org.tmoerman.brassica._
import org.tmoerman.brassica.algo.ComputeXGBoostOptimizedHyperParams._
import org.tmoerman.brassica.tuning.CV.makeCVSets
import org.tmoerman.brassica.util.BreezeUtils.toDMatrix

/**
  * @author Thomas Moerman
  */
case class ComputeXGBoostOptimizedHyperParams(params: XGBoostOptimizationParams)
                                             (regulators: List[Gene],
                                              regulatorCSC: CSCMatrix[Expression],
                                              partitionIndex: Int) extends PartitionTask[OptimizedHyperParams] {


  import params._

  private[this] val cvSets = makeCVSets(nrFolds, regulatorCSC.rows, seed + partitionIndex)

  /**
    * @return Returns the optimized hyperparameters for one ExpressionByGene instance.
    */
  override def apply(expressionByGene: ExpressionByGene): Iterable[OptimizedHyperParams] = {
    val rng = random(seed + partitionIndex)
    val targetGene = expressionByGene.gene

    // println(s"-> target: $targetGene, regulator: $targetIsRegulator, partition: $partitionIndex")

    val (nFoldDMatrices, disposeAll) =
      makeNFoldDMatrices(
        expressionByGene,
        regulators,
        regulatorCSC,
        cvSets)

    // optimize the params for the current n-fold CV sets
    val trials =
      (1 to nrTrials)
        .map(trial => {
          val sampledParams = boosterParamSpace.map{ case (key, generator) => (key, generator(rng)) }
          val loss = computeLoss(nFoldDMatrices, sampledParams, params)

          println(s"target: $targetGene \t trial: $trial \t loss: $loss \t $sampledParams")

          (sampledParams, loss)
        })

    disposeAll()

    if (onlyBest) {
      val (sampledParams, loss) = trials.minBy(_._2)

      Iterable(toOptimizedHyperParams(targetGene, sampledParams, loss))
    } else {
      trials.map{ case (sampledParams, loss) => toOptimizedHyperParams(targetGene, sampledParams, loss)}
    }
  }

  private[this] def toOptimizedHyperParams(targetGene: Gene, sampledParams: BoosterParams, loss: Float) = {
    val sorted   = sampledParams.toSeq.sortBy(_._1)
    val keys     = sorted.map(_._1).mkString(",")
    val values   = sorted.map(_._2.toString.toDouble).toArray

    OptimizedHyperParams(
      target = targetGene,
      metric = evalMetric,
      nrBoostingRounds = nrRounds,
      loss = loss,
      keys = keys,
      values = dense(values))
  }

  override def dispose(): Unit = {}

}

object ComputeXGBoostOptimizedHyperParams {

  type CVSet  = (Array[CellIndex], Array[CellIndex])

  private[this] val NAMES = Array("train", "test")

  def makeNFoldDMatrices(expressionByGene: ExpressionByGene,
                         regulators: List[Gene],
                         regulatorCSC: CSCMatrix[Expression],
                         cvSets: List[CVSet]): (List[(DMatrix, DMatrix)], () => Unit) = {

    val targetGene = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    // remove the target gene column if target gene is a regulator
    val cleanedDMatrixGenesToIndices =
      regulators
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene } // remove the target from the predictors

    // slice the target gene column from the regulator CSC matrix and create a new DMatrix
    val regulatorDMatrix =
      if (targetIsRegulator) {
        val regulatorCSCMinusTarget = regulatorCSC(0 until regulatorCSC.rows, cleanedDMatrixGenesToIndices.map(_._2))

        toDMatrix(regulatorCSCMinusTarget)
      } else {
        toDMatrix(regulatorCSC)
      }

    regulatorDMatrix.setLabel(expressionByGene.response)

    val nFoldDMatrices = toNFoldDMatrices(regulatorDMatrix, cvSets)

    val disposeAll = () => {
      regulatorDMatrix.delete()
      dispose(nFoldDMatrices)
    }

    (nFoldDMatrices, disposeAll)
  }

  def toNFoldDMatrices(matrix: DMatrix, cvSets: List[(Array[CellIndex], Array[CellIndex])]) =
    cvSets.map{ case (trainIndices, testIndices) => (matrix.slice(trainIndices), matrix.slice(testIndices)) }

  def dispose(matrices: List[(DMatrix, DMatrix)]): Unit =
    matrices.foreach{ case (a, b) => {
      a.delete()
      b.delete()
    }}

  def computeLoss(nFoldDMatrices: List[(DMatrix, DMatrix)],
                  sampledBoosterParams: BoosterParams,
                  optimizationParams: XGBoostOptimizationParams): Float = {

    import optimizationParams._

    val sampledBoosterParamsWithDefaults =
      sampledBoosterParams +
        ("eval_metric" -> evalMetric) +
        ("silent" -> 1) +
        ("nthread" -> 1)

    val cvPacks: List[(DMatrix, DMatrix, Booster)] =
      nFoldDMatrices
        .map{ case (train, test) => (train, test, createBooster(sampledBoosterParamsWithDefaults, train, test)) }

    // TODO early stopping strategy or elbow calculation.
    val foldResultsPerRound =
      (0 until nrRounds)
        .map(round => {
          val foldResults =
            cvPacks
              .map{ case (train, test, booster) =>
                val train4j = inner(train)
                val test4j  = inner(test)
                val matrices = Array(train4j, test4j)

                booster.update(train4j, round)
                if (round == nrRounds-1) booster.evalSet(matrices, NAMES, round) else EMPTY
              }

          (round, foldResults)
        })

    // TODO also return round nr ~ last for now
    val (lastRound, lastFoldResults) = foldResultsPerRound.last
    val (lastTrainingLoss, lastTestLoss) = toEvalScores(lastFoldResults)

    // dispose boosters
    cvPacks.map(_._3).foreach(_.dispose())

    lastTestLoss
  }

  /**
    * @param foldResults
    * @return Return the
    */
  def toEvalScores(foldResults: Iterable[String]): (Float, Float) = {
    val averageEvalScores =
      foldResults
        .flatMap(foldResult => {
          foldResult
            .split("\t")
            .drop(1) // drop the index
            .map(_.split(":") match {
            case Array(key, value) => (key, value.toFloat)
          })})
        .groupBy(_._1)
        .mapValues(x => x.map(_._2).sum / x.size)

    (averageEvalScores("train-rmse"), averageEvalScores("test-rmse"))
  }

}