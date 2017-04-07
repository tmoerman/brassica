package org.tmoerman.brassica.algo

import java.lang.Math.min

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.java.Booster
import ml.dmlc.xgboost4j.java.JXGBoostAccess.createBooster
import ml.dmlc.xgboost4j.scala.DMatrix
import ml.dmlc.xgboost4j.scala.XGBoostAccess.inner
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

  private[this] val cvSets = makeCVSets(nrFolds, regulatorCSC.rows, seed)

  /**
    * @param expressionByGene The target gene and expression.
    * @return Returns the optimized hyper parameters for one ExpressionByGene instance.
    */
  override def apply(expressionByGene: ExpressionByGene): Iterable[OptimizedHyperParams] = {
    val rng = random(seed)
    val targetGene = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    println(s"-> target: $targetGene, regulator: $targetIsRegulator, partition: $partitionIndex")

    val (nFoldDMatrices, disposeAll) = makeNFoldDMatrices(expressionByGene, regulators, regulatorCSC, cvSets)

    // optimize the params for the current n-fold CV sets
    val trials =
      (1 to nrTrials)
        .map(trial => {
          val sampledParams  = boosterParamSpace.map{ case (key, generator) => (key, generator(rng)) }
          val (rounds, loss) = computeCVLoss(nFoldDMatrices, sampledParams, params)

          println(s"target: $targetGene \t trial: $trial \t loss: $loss \t $sampledParams \t partition: $partitionIndex")

          (sampledParams, (rounds, loss))
        })

    disposeAll()

    val best = trials.minBy{ case (_, (_, loss)) => loss }

    if (onlyBestTrial) {
      val (sampledParams, (rounds, loss)) = best

      val result = toOptimizedHyperParams(targetGene, sampledParams, rounds, loss, best = true, params)

      Iterable(result)
    } else {
      trials
        .map{ case trial @ (sampledParams, (rounds, loss)) =>
          toOptimizedHyperParams(targetGene, sampledParams, rounds, loss, trial == best, params)
        }
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
                    optimizationParams: XGBoostOptimizationParams): (Round, Loss) = {

    import optimizationParams._

    // we need the same boosters for all rounds
    val foldsAndBoosters: List[(DMatrix, DMatrix, Booster)] =
      nFoldDMatrixPairs
        .map{ case (train, test) =>
          val booster = createBooster(withDefaults(sampledBoosterParams, optimizationParams), train, test)
          (train, test, booster)
        }

    // compute test losses
    val testLossesByRound: Stream[(Round, Loss)] =
      (0 until maxNrRounds)
        .toStream
        .map(round => {
          val roundResults =
            foldsAndBoosters
              .map{ case (train, test, booster) =>
                val train4j = inner(train)
                val test4j  = inner(test)
                val mats    = Array(train4j, test4j)

                booster.update(train4j, round)
                booster.evalSet(mats, NAMES, round)}

          val (_, testLoss) = toLossScores(roundResults)

          (round, testLoss)})

    // infer a reasonable round nr to stop early
    val (round, loss) = takeUntilEarlyStop(testLossesByRound, optimizationParams)

    // dispose boosters
    foldsAndBoosters.map(_._3).foreach(_.dispose())

    // return result
    (round, loss)
  }

  /**
    * @return Returns sampled Booster params with extra defaults.
    */
  def withDefaults(sampledBoosterParams: BoosterParams,
                   optimizationParams: XGBoostOptimizationParams): BoosterParams = {

    import optimizationParams._

    val base = sampledBoosterParams + ("eval_metric" -> evalMetric) + ("silent" -> 1)

    if (parallel) base else base + ("nthread" -> 1)
  }

  /**
    * Compute test losses until the delta between the window head and tail is smaller than a configured early stop delta.
    *
    * @param testLossesByRound A lazy Stream of test losses by round.
    * @param optimizationParams The optimization params.
    * @return Returns the last or early pair of test loss by round.
    */
  def takeUntilEarlyStop(testLossesByRound: Stream[(Round, Loss)],
                         optimizationParams: XGBoostOptimizationParams): (Round, Loss) = {

    import optimizationParams._

    testLossesByRound
      .sliding(earlyStopWindow, 1)
      .takeWhile(window => {
        val windowDelta = window.head._2 - window.last._2

        windowDelta > earlyStopDelta
      })
      .map(window => window(window.length / 2))
      .toIterable
      .last
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
                             round: Round,
                             loss: Loss,
                             best: Boolean,
                             optimizationParams: XGBoostOptimizationParams): OptimizedHyperParams = {
    import optimizationParams._

    val sorted = sampledParams.toSeq.sortBy(_._1)
    val keys   = sorted.map(_._1).mkString(",")
    val values = sorted.map(_._2.toString.toDouble).toArray

    OptimizedHyperParams(
      target = targetGene,
      metric = evalMetric,
      rounds = round,
      loss   = loss,
      //keys   = keys,
      //values = dense(values)
      eta               = sampledParams("eta")              .asInstanceOf[Double],
      max_depth         = sampledParams("max_depth")        .asInstanceOf[Int],
      min_child_weight  = sampledParams("min_child_weight") .asInstanceOf[Double],
      gamma             = sampledParams("gamma")            .asInstanceOf[Double],
      subsample         = sampledParams("subsample")        .asInstanceOf[Double],
      colsample_bytree  = sampledParams("colsample_bytree") .asInstanceOf[Double],
      best   = best)
  }

  /**
    * @param nrFolds The number of CV folds.
    * @param nrSamples The number of samples to partition across folds.
    * @param seed A random seed.
    * @return Returns a Map of (train, test) sets by fold id.
    */
  def makeCVSets(nrFolds: Count,
                 nrSamples: Count,
                 seed: Long = DEFAULT_SEED): List[CVSet] = {

    val foldSlices = makeFoldSlices(nrFolds, nrSamples, seed)

    foldSlices
      .keys
      .toList
      .map(fold => {
        val (train, test) = foldSlices.partition(_._1 != fold)

        (train.values.flatten.toArray, test.values.flatten.toArray)})
  }

  type FoldNr = Int

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