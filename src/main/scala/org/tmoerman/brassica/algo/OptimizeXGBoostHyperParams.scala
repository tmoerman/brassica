package org.tmoerman.brassica.algo

import java.lang.Math.min

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.java.Booster
import ml.dmlc.xgboost4j.java.JXGBoostAccess.createBooster
import ml.dmlc.xgboost4j.scala.DMatrix
import ml.dmlc.xgboost4j.scala.XGBoostAccess.inner
import org.apache.spark.ml.linalg.Vectors.dense
import org.tmoerman.brassica._
import org.tmoerman.brassica.algo.OptimizeXGBoostHyperParams._
import org.tmoerman.brassica.util.BreezeUtils.toDMatrix

/**
  * PartitionTask implementation for XGBoost hyper parameter optimization.
  *
  * @author Thomas Moerman
  */
case class OptimizeXGBoostHyperParams(params: XGBoostOptimizationParams)
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
          val cvLoss        = computeCVLoss(nFoldDMatrices, sampledParams, params)

          println(s"target: $targetGene \t trial: $trial \t loss: $cvLoss \t $sampledParams")

          (sampledParams, cvLoss)
        })

    disposeAll()

    if (onlyBestTrial) {
      val (sampledParams, loss) = trials.minBy(_._2)

      Iterable(toOptimizedHyperParams(targetGene, sampledParams, loss, params))
    } else {
      trials.map{ case (sampledParams, loss) => toOptimizedHyperParams(targetGene, sampledParams, loss, params)}
    }
  }

  override def dispose(): Unit = {}

}

/**
  * Companion object exposing stateless functions.
  */
object OptimizeXGBoostHyperParams {

  type CVSet  = (Array[CellIndex], Array[CellIndex])

  private[this] val NAMES = Array("train", "test")

  /**
    * @return Returns a tuple of
    *         - list of pairs of n-fold DMatrix instances
    *         - a dispose function.
    */
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

    val nFoldDMatrices = sliceToNFoldDMatrixPairs(regulatorDMatrix, cvSets)

    val disposeMatrices = () => {
      regulatorDMatrix.delete()
      dispose(nFoldDMatrices)
    }

    (nFoldDMatrices, disposeMatrices)
  }

  def sliceToNFoldDMatrixPairs(matrix: DMatrix, cvSets: List[CVSet]): List[(DMatrix, DMatrix)] =
    cvSets
      .map{ case (trainIndices, testIndices) => (matrix.slice(trainIndices), matrix.slice(testIndices)) }

  def dispose(matrices: List[(DMatrix, DMatrix)]): Unit =
    matrices.foreach{ case (a, b) => {
      a.delete()
      b.delete()
    }}

  /**
    * @param nFoldDMatrixPairs The n-fold matrices for crossValidation.
    * @param sampledBoosterParams A candidate sampled set of XGBoost regression BoosterParams.
    * @param optimizationParams The XGBoost optimization parameters.
    * @return Returns the loss of the specified sampled BoosterParams over the n-fold matrices,
    *         in function of a specified evaluation metric (usually RMSE for regression).
    */
  def computeCVLoss(nFoldDMatrixPairs: List[(DMatrix, DMatrix)],
                    sampledBoosterParams: BoosterParams,
                    optimizationParams: XGBoostOptimizationParams): Float = {

    import optimizationParams._

    // we need the same boosters for all rounds
    val cvPacks: List[(DMatrix, DMatrix, Booster)] =
      nFoldDMatrixPairs
        .map{ case (train, test) => (train, test, createBooster(withDefaults(sampledBoosterParams, optimizationParams), train, test)) }

    // TODO early stopping strategy or elbow calculation.
    val resultsPerRound =
      (0 until nrRounds)
        .map(round => {
          val roundResults =
            cvPacks
              .map{ case (train, test, booster) =>
                val train4j = inner(train)
                val test4j  = inner(test)
                val matrices = Array(train4j, test4j)

                booster.update(train4j, round)
                booster.evalSet(matrices, NAMES, round)
              }

          (round, roundResults)
        })

    // TODO also return round nr ~ last for now
    val (lastRound, lastRoundResults) = resultsPerRound.last
    val (lastTrainingLoss, lastTestLoss) = toLossScores(lastRoundResults)

    // dispose boosters
    cvPacks.map(_._3).foreach(_.dispose())

    lastTestLoss
  }

  /**
    * @param sampledBoosterParams
    * @param params
    * @return Returns sampled Booster params with extra defaults.
    */
  def withDefaults(sampledBoosterParams: BoosterParams, params: XGBoostOptimizationParams): BoosterParams = {
    val base = sampledBoosterParams + ("eval_metric" -> params.evalMetric) + ("silent" -> 1)

    if (params.parallel) base else base + ("nthread" -> 1)
  }

  /**
    * @param roundResults
    * @return Return the train and test CV evaluation scores.
    */
  def toLossScores(roundResults: Iterable[String]): (Loss, Loss) = {
    val averageEvalScores =
      roundResults
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

  /**
    * @return Returns the structured form of a sampled BoosterParams instance.
    */
  def toOptimizedHyperParams(targetGene: Gene,
                             sampledParams: BoosterParams,
                             loss: Loss,
                             optimizationParams: XGBoostOptimizationParams): OptimizedHyperParams = {
    import optimizationParams._

    val sorted   = sampledParams.toSeq.sortBy(_._1)
    val keys     = sorted.map(_._1).mkString(",")
    val values   = sorted.map(_._2.toString.toDouble).toArray

    OptimizedHyperParams(
      target = targetGene,
      metric = evalMetric,
      rounds = nrRounds,
      loss = loss,
      keys = keys,
      values = dense(values))
  }

  type FoldNr = Int

  /**
    * @param nrFolds
    * @param nrSamples
    * @param seed
    * @return Returns a Map of (train, test) sets by fold id.
    */
  def makeCVSets(nrFolds: Count,
                 nrSamples: Count,
                 seed: Long = DEFAULT_SEED): List[(Array[CellIndex], Array[CellIndex])] = {

    val foldSlices = makeFoldSlices(nrFolds, nrSamples, seed)

    foldSlices
      .keys
      .toList
      .map(fold => {
        val (train, test) = foldSlices.partition(_._1 != fold)

        (train.values.flatten.toArray, test.values.flatten.toArray)})
  }

  /**
    * @param nrFolds The nr of folds.
    * @param nrSamples The nr of samples to slice into folds.
    * @param seed A seed for the random number generator.
    * @return Returns a Map of cell indices by fold id.
    */
  def makeFoldSlices(nrFolds: Count,
                     nrSamples: Count,
                     seed: Long = DEFAULT_SEED): Map[FoldNr, List[CellIndex]] = {

    assert(nrFolds > 1, s"nr folds must be greater than 1 (specified: $nrFolds)")

    assert(nrSamples > 0, s"nr samples must be greater than 0 (specified: $nrSamples)")

    val denominator = min(nrFolds, nrSamples)

    random(seed)
      .shuffle((0 until nrSamples).toList)
      .zipWithIndex
      .map{ case (cellIndex, idx) => (cellIndex, idx % denominator) }
      .groupBy{ case (_, fold) => fold }
      .mapValues(_.map(_._1).sorted)
  }

}