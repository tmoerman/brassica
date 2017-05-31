package org.aertslab.grnboost.algo

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import org.aertslab.grnboost._
import org.aertslab.grnboost.util.BreezeUtils._
import InferXGBoostRegulations._

/**
  * @author Thomas Moerman
  */
case class InferRegulationsIterated(params: XGBoostRegressionParams,
                                    regulators: List[Gene],
                                    regulatorCSC: CSCMatrix[Expression]) {

  def apply(expressionByGene: ExpressionByGene): Iterable[RawRegulation] = {

    val targetGene        = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    println(s"-> target: $targetGene \t regulator: $targetIsRegulator")

    val (matrix, result) = if (targetIsRegulator) {
      val targetColumnIndex = regulators.zipWithIndex.find(_._1 == targetGene).get._2

      val labeledMatrix =
        regulatorCSC
          .dropColumn(targetColumnIndex)
          .iterateToLabeledDMatrix(expressionByGene.response)

      val cleanRegulators = regulators.filterNot(_ == targetGene)

      val result = inferRegulations(params, targetGene, cleanRegulators, labeledMatrix)

      (labeledMatrix, result)
    } else {
      val labeledMatrix =
        regulatorCSC
          .iterateToLabeledDMatrix(expressionByGene.response)

      val result = inferRegulations(params, targetGene, regulators, labeledMatrix)

      (labeledMatrix, result)
    }

    matrix.delete()

    result
  }

}

case class ComputeCVLoss(params: XGBoostRegressionParams)
                        (regulators: List[Gene],
                         regulatorCSC: CSCMatrix[Expression],
                         partitionIndex: Int) extends PartitionTask[LossByRound] {

  private[this] val cachedRegulatorDMatrix = regulatorCSC.copyToUnlabeledDMatrix

  /**
    * @param expressionByGene The current target gene and its expression vector.
    * @return Returns the inferred Regulation instances for one ExpressionByGene instance.
    */
  override def apply(expressionByGene: ExpressionByGene): Iterable[LossByRound] = {
    val targetGene        = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    println(s"-> target: $targetGene \t regulator: $targetIsRegulator \t partition: $partitionIndex")

    // TODO extract template method

    if (targetIsRegulator) {
      // drop the target gene column from the regulator CSC matrix and create a new DMatrix
      val targetColumnIndex = regulators.zipWithIndex.find(_._1 == targetGene).get._2
      val cleanRegulatorDMatrix = regulatorCSC.dropColumn(targetColumnIndex).copyToUnlabeledDMatrix
      val cleanRegulators = regulators.filterNot(_ == targetGene)

      // set the response labels and train the model
      cleanRegulatorDMatrix.setLabel(expressionByGene.response)

      val result = computeCVLoss(params, targetGene, cleanRegulators, cleanRegulatorDMatrix)

      cleanRegulatorDMatrix.delete()

      result
    } else {
      // set the response labels and train the model
      cachedRegulatorDMatrix.setLabel(expressionByGene.response)

      val result = computeCVLoss(params, targetGene, regulators, cachedRegulatorDMatrix)

      result
    }
  }

  override def dispose(): Unit = {
    cachedRegulatorDMatrix.delete()
  }

}

case class InferXGBoostRegulations(params: XGBoostRegressionParams)
                                  (regulators: List[Gene],
                                   regulatorCSC: CSCMatrix[Expression],
                                   partitionIndex: Int) extends PartitionTask[RawRegulation] {

  private[this] val cachedRegulatorDMatrix = regulatorCSC.copyToUnlabeledDMatrix

  /**
    * @param expressionByGene The current target gene and its expression vector.
    * @return Returns the inferred Regulation instances for one ExpressionByGene instance.
    */
  override def apply(expressionByGene: ExpressionByGene): Iterable[RawRegulation] = {
    val targetGene        = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    println(s"-> target: $targetGene \t regulator: $targetIsRegulator \t partition: $partitionIndex")

    if (targetIsRegulator) {
      // drop the target gene column from the regulator CSC matrix and create a new DMatrix
      val targetColumnIndex = regulators.zipWithIndex.find(_._1 == targetGene).get._2
      val cleanRegulatorDMatrix = regulatorCSC.dropColumn(targetColumnIndex).copyToUnlabeledDMatrix
      val cleanRegulators = regulators.filterNot(_ == targetGene)

      // set the response labels and train the model
      cleanRegulatorDMatrix.setLabel(expressionByGene.response)

      val result = inferRegulations(params, targetGene, cleanRegulators, cleanRegulatorDMatrix)

      cleanRegulatorDMatrix.delete()

      result
    } else {
      // set the response labels and train the model
      cachedRegulatorDMatrix.setLabel(expressionByGene.response)

      val result = inferRegulations(params, targetGene, regulators, cachedRegulatorDMatrix)

      result
    }
  }

  override def dispose(): Unit = {
    cachedRegulatorDMatrix.delete()
  }

}

object InferXGBoostRegulations {

  type TreeDump  = String
  type ModelDump = Seq[TreeDump]

  def computeCVLoss(params: XGBoostRegressionParams,
                    targetGene: Gene,
                    regulators: List[Gene],
                    regulatorDMatrix: DMatrix): Seq[LossByRound] = {
    import params._

    XGBoost
      .crossValidation(regulatorDMatrix, boosterParams.withDefaults, nrRounds, nrFolds)
      .map(_.split("\t").drop(1).map(_.split(":")(1).toFloat))
      .zipWithIndex
      .map{ case (Array(train, test), i) => LossByRound(targetGene, train, test, i) }
  }

  def inferRegulations(params: XGBoostRegressionParams,
                       targetGene: Gene,
                       regulators: List[Gene],
                       regulatorDMatrix: DMatrix): Seq[RawRegulation] = {
    import params._

    val booster = XGBoost.train(regulatorDMatrix, boosterParams.withDefaults, nrRounds)

    val regulations = toRawRegulations(targetGene, regulators, booster, params)

    booster.dispose

    regulations
  }

  /**
    * @param targetGene
    * @param regulators
    * @param booster
    * @return Returns the raw scores for regulation.
    */
  def toRawRegulations(targetGene: Gene,
                       regulators: List[Gene],
                       booster: Booster,
                       params: XGBoostRegressionParams): Seq[RawRegulation] = {

    val boosterModelDump = booster.getModelDump(withStats = true).toSeq

    aggregateGainByGene(params)(boosterModelDump)
      .toSeq
      .map{ case (geneIndex, normalizedGain) =>
        RawRegulation(regulators(geneIndex), targetGene, normalizedGain)
      }
  }

  /**
    * See Python implementation:
    *   https://github.com/dmlc/xgboost/blob/d943720883f0e70ce1fbce809e373908b47bd506/python-package/xgboost/core.py#L1078
    *
    * @param modelDump Trained booster or tree model dump.
    * @return Returns the feature importance metrics parsed from all trees (amount == nr boosting rounds) in the
    *         specified trained booster model.
    */
  def aggregateGainByGene(params: XGBoostRegressionParams)(modelDump: ModelDump): Map[GeneIndex, Gain] =
    modelDump
      .flatMap(parseGainScores)
      .foldLeft(Map[GeneIndex, Gain]() withDefaultValue 0f) { case (acc, (geneIndex, gain)) =>
        acc.updated(geneIndex, acc(geneIndex) + gain)
      }

  /**
    * @param treeDump
    * @return Returns the feature importance metrics parsed from one tree.
    */
  def parseGainScores(treeDump: TreeDump): Array[(GeneIndex, Gain)] =
    treeDump
      .split("\n")
      .flatMap(_.split("\\[") match {
        case Array(_) => Nil // leaf node, ignore
        case array =>
          array(1).split("\\]") match {
            case Array(left, right) =>
              val geneIndex = left.split("<")(0).substring(1).toInt

              // TODO parse other scores
              val gain     = right.split(",").find(_.startsWith("gain")).map(_.split("=")(1)).get.toFloat

              (geneIndex, gain) :: Nil
          }
      })

}