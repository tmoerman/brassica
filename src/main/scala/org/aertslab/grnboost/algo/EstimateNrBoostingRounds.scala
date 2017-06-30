package org.aertslab.grnboost.algo

import java.lang.Math.min

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.java.XGBoostUtils.createBooster
import ml.dmlc.xgboost4j.scala.DMatrix
import ml.dmlc.xgboost4j.scala.XGBoostConversions._
import org.aertslab.grnboost._
import org.aertslab.grnboost.algo.EstimateNrBoostingRounds._
import org.aertslab.grnboost.util.BreezeUtils._
import org.aertslab.grnboost.util.TriangleRegularization.inflectionPointIndex

/**
  * @author Thomas Moerman
  */
case class EstimateNrBoostingRounds(params: XGBoostRegressionParams)
                                   (regulators: List[Gene],
                                    regulatorCSC: CSCMatrix[Expression],
                                    partitionIndex: Int) extends PartitionTask[RoundsEstimation] {
  import params._

  private[this] val cachedRegulatorDMatrix = regulatorCSC.copyToUnlabeledDMatrix

  /**
    * @param expressionByGene The current target gene and its expression vector.
    * @return Returns the inferred Regulation instances for one ExpressionByGene instance.
    */
  override def apply(expressionByGene: ExpressionByGene): Iterable[RoundsEstimation] = {
    val targetGene        = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    println(s"estimating nr boosting rounds -> target: $targetGene \t regulator: $targetIsRegulator \t partition: $partitionIndex")

    val foldIndices = indicesByFold(nrFolds, regulatorCSC.rows, seed)

    if (targetIsRegulator) {
      // drop the target gene column from the regulator CSC matrix and create a new DMatrix
      val targetColumnIndex = regulators.zipWithIndex.find(_._1 == targetGene).get._2
      val cleanRegulatorDMatrix = regulatorCSC.dropColumn(targetColumnIndex).copyToUnlabeledDMatrix

      cleanRegulatorDMatrix.setLabel(expressionByGene.response)

      val result =
        estimateRoundsPerFold(
          nrFolds,
          targetGene,
          params,
          cleanRegulatorDMatrix,
          foldIndices)

      cleanRegulatorDMatrix.delete()

      result
    } else {
      cachedRegulatorDMatrix.setLabel(expressionByGene.response)

      val result =
        estimateRoundsPerFold(
          nrFolds,
          targetGene,
          params,
          cachedRegulatorDMatrix,
          foldIndices)

      result
    }
  }

  override def dispose(): Unit = {
    cachedRegulatorDMatrix.delete()
  }

}

/**
  * Pure functions factored out for testing and elegant composition purposes.
  */
object EstimateNrBoostingRounds {

  type FoldNr = Int

  val MAX_ROUNDS  = 5000
  val INC_ROUNDS  = 50
  val SKIP_ROUNDS = 5

  /**
    * @param nrFolds The nr of CV folds.
    * @param targetGene The target gene.
    * @param params The regression parameters.
    * @param regulatorDMatrix The DMatrix of regulator expression values.
    * @param indicesByFold A Map of cell indices by fold nr.
    * @param allCVSets If true, make CV sets with respect to all folds, otherwise only one CV set is constructed in
    *                  function of the fold slices.
    * @param maxRounds The maximum nr of boosting rounds to try.
    * @param incRounds The increment of boosting rounds for lazily finding the inflection point.
    * @return Returns a Seq of RoundsEstimation instances.
    */
  def estimateRoundsPerFold(nrFolds: Int,
                            targetGene: Gene,
                            params: XGBoostRegressionParams,
                            regulatorDMatrix: DMatrix,
                            indicesByFold: Map[FoldNr, List[CellIndex]],
                            allCVSets: Boolean = false,
                            maxRounds: Int = MAX_ROUNDS,
                            incRounds: Int = INC_ROUNDS): Seq[RoundsEstimation] =

    (if (allCVSets) 0 until nrFolds else 0 :: Nil)
      .flatMap(foldNr => {
        val (train, test) = cvSet(foldNr, indicesByFold, regulatorDMatrix)

        val estimation = estimateFoldRounds(foldNr, targetGene, params, train, test, maxRounds, incRounds)

        train.delete()
        test.delete()

        estimation
      })

  /**
    * @param foldNr The nr of the fold.
    * @param params Params.
    * @param train Training DMatrix.
    * @param test Test DMatrix
    * @param maxRounds The maximum nr of boosting rounds to try.
    * @param incRounds The increment of boosting rounds for lazily finding the inflection point.
    * @return Returns an Option of RoundsEstimation.
    */
  def estimateFoldRounds(foldNr: FoldNr,
                         targetGene: Gene,
                         params: XGBoostRegressionParams,
                         train: DMatrix,
                         test: DMatrix,
                         maxRounds: Int = MAX_ROUNDS,
                         incRounds: Int = INC_ROUNDS): Option[RoundsEstimation] = {

    import params._

    lossesByRoundReductions(boosterParams, train, test, maxRounds, incRounds)
      .flatMap(lossesByRound =>
        inflectionPointIndex(lossesByRound.map{ case (_, (_, testLoss)) => testLoss })
          .map(lossesByRound(_))
          .toSeq)
      .headOption
      .map{ case (round, (_, testLoss)) => RoundsEstimation(foldNr, targetGene, testLoss, round) }
  }

  /**
    * Function factored out for testing purposes.
    *
    * NOTE: "Reductions" is an FP concept, see: https://clojuredocs.org/clojure.core/reductions.
    * In Scala, it is implemented by the scanLeft function.
    * We use it to reduce a Stream of values into a Stream of lists of values.
    *
    *   E.g.
    *     ()
    *     (1, 2, 3)
    *     (1, 2, 3, 4, 5, 6)
    *     (1, 2, 3, 4, 5, 6, 7, 8, 9)
    *     ...
    *
    * @param boosterParams Booster parameter map.
    * @param train The training DMatrix.
    * @param test The test DMatrix
    * @param maxRounds The maximum nr of boosting rounds to consider.
    * @param incRounds The "step" size, every consecutive list has incRounds more elements than the previous one.
    * @return Returns a Stream of Lists of Losses by boosting round. Every subsequent list contains the previous one
    *         as a sublist.
    */
  def lossesByRoundReductions(boosterParams: BoosterParams,
                              train: DMatrix,
                              test: DMatrix,
                              maxRounds: Int = MAX_ROUNDS,
                              incRounds: Int = INC_ROUNDS): Stream[List[(Round, (Loss, Loss))]] = {

    val booster = createBooster(boosterParams, train, test)

    val mats   = Array(train, test)
    val names  = Array("train", "test")
    val metric = boosterParams.getOrElse(XGB_METRIC, DEFAULT_EVAL_METRIC).toString

    val ZERO = List[(Round, (Loss, Loss))]()

    /**
      * @param nextRounds The number of boosting rounds to effect.
      * @return Returns a List of losses by round.
      */
    def boostAndExtractLossesByRound(nextRounds: Iterable[Round]): List[(Round, (Loss, Loss))] =
      nextRounds
        .map(round => {
          booster.update(train, round)

          val evalSet = booster.evalSet(mats, names, round)
          val lossScores = parseLossScores(evalSet, metric)

          (round, lossScores)
        })
        .toList

    Stream
      .range(0, maxRounds)
      .sliding(incRounds, incRounds)
      .toStream // necessary to make next step lazy
      .scanLeft(ZERO){ (acc, nextRounds) => acc ::: boostAndExtractLossesByRound(nextRounds) }
  }

  /**
    * @param modelEvaluation A String containing the booster model evaluation.
    * @return Returns the train and test loss scores, parsed from the model evaluation String.
    */
  def parseLossScores(modelEvaluation: String, evalMetric: String): (Loss, Loss) = {
    val losses =
      modelEvaluation
        .split("\t")
        .drop(1)
        .map(_.split(":") match {
          case Array(key, value) => (key, value.toFloat)
        })
        .toMap

    (losses(s"train-$evalMetric"), losses(s"test-$evalMetric"))
  }

  /**
    * @param foldNr The current fold nr.
    * @param indicesByFold The cell indices for each fold, by fold nr.
    * @param matrix The DMatrix to slice into training and test matrices.
    * @return Returns a pair of Arrays of cell indices that represent the cell IDs of one CV set.
    *         The fold slice with nr equal to foldNr becomes the test matrix, whereas the rest of the slices
    *         are used for the training matrix.
    */
  def cvSet(foldNr: FoldNr,
            indicesByFold: Map[FoldNr, List[CellIndex]],
            matrix: DMatrix): (DMatrix, DMatrix) = {

    val (trainSlices, testSlice) = indicesByFold.partition(_._1 != foldNr)

    val trainIndices = trainSlices.values.flatten.toArray
    val testIndices  = testSlice.values.flatten.toArray

    (matrix.slice(trainIndices), matrix.slice(testIndices))
  }

  /**
    * @param nrFolds The nr of folds.
    * @param nrSamples The nr of samples to slice into folds.
    * @param seed A seed for the random number generator.
    * @return Returns a Map of Lists of cell indices by fold id.
    */
  def indicesByFold(nrFolds: Count,
                    nrSamples: Count,
                    seed: Seed = DEFAULT_SEED): Map[FoldNr, List[CellIndex]] = {

    assert(nrFolds   > 1, s"nr folds must be greater than 1 (specified: $nrFolds)")
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