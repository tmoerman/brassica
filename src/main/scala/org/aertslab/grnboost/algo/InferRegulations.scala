package org.aertslab.grnboost.algo

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import org.aertslab.grnboost._
import org.aertslab.grnboost.util.BreezeUtils._
import InferRegulations._

/**
  * @author Thomas Moerman
  */
case class InferRegulationsIterated(params: XGBoostRegressionParams,
                                    regulators: List[Gene],
                                    regulatorCSC: CSCMatrix[Expression]) {

  def apply(expressionByGene: ExpressionByGene): Iterable[Regulation] = {

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

case class InferRegulations(params: XGBoostRegressionParams)
                           (regulators: List[Gene],
                            regulatorCSC: CSCMatrix[Expression],
                            partitionIndex: Int) extends PartitionTask[Regulation] {

  private[this] val cachedRegulatorDMatrix = regulatorCSC.copyToUnlabeledDMatrix

  /**
    * @param expressionByGene The current target gene and its expression vector.
    * @return Returns the inferred Regulation instances for one ExpressionByGene instance.
    */
  override def apply(expressionByGene: ExpressionByGene): Iterable[Regulation] = {
    val targetGene        = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    println(s"-> target: $targetGene \t regulator: $targetIsRegulator \t partition: $partitionIndex")

    if (targetIsRegulator) {
      // drop the target gene column from the regulator CSC matrix and create a new DMatrix
      val targetColumnIndex = regulators.zipWithIndex.find(_._1 == targetGene).get._2
      val cleanRegulatorDMatrix = regulatorCSC.dropColumn(targetColumnIndex).copyToUnlabeledDMatrix
      val cleanRegulators = regulators.filterNot(_ == targetGene)

      cleanRegulatorDMatrix.setLabel(expressionByGene.response)

      val result = inferRegulations(params, targetGene, cleanRegulators, cleanRegulatorDMatrix)

      cleanRegulatorDMatrix.delete()

      result
    } else {
      cachedRegulatorDMatrix.setLabel(expressionByGene.response)

      val result = inferRegulations(params, targetGene, regulators, cachedRegulatorDMatrix)

      result
    }
  }

  override def dispose(): Unit = {
    cachedRegulatorDMatrix.delete()
  }

}

object InferRegulations {

  def inferRegulations(params: XGBoostRegressionParams,
                       targetGene: Gene,
                       regulators: List[Gene],
                       regulatorDMatrix: DMatrix): Seq[Regulation] = {
    import params._

    val booster = XGBoost.train(regulatorDMatrix, boosterParams.withDefaults, nrRounds)

    val regulations = toRegulations(targetGene, regulators, booster, params)

    booster.dispose

    regulations
  }

  /**
    * @param targetGene
    * @param regulators
    * @param booster
    * @return Returns the raw scores for regulation.
    */
  def toRegulations(targetGene: Gene,
                    regulators: List[Gene],
                    booster: Booster,
                    params: XGBoostRegressionParams): Seq[Regulation] = {

    val boosterModelDump = booster.getModelDump(withStats = true).toSeq

    val scoresByGene = aggregateScoresByGene(params)(boosterModelDump)

    scoresByGene
      .toSeq
      .map{ case (geneIndex, scores) =>
        Regulation(regulators(geneIndex), targetGene, scores.gain)
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
  def aggregateScoresByGene(params: XGBoostRegressionParams)(modelDump: ModelDump): Map[GeneIndex, Scores] =
    modelDump
      .flatMap(parseImportanceScores)
      .foldLeft(Map[GeneIndex, Scores]() withDefaultValue Scores.ZERO) { case (acc, (geneIndex, scores)) =>
        acc.updated(geneIndex, acc(geneIndex) |+| scores)
      }

  /**
    * @param treeDump
    * @return Returns the feature importance metrics parsed from one tree.
    */
  def parseImportanceScores(treeDump: TreeDump): Array[(GeneIndex, Scores)] =
    treeDump
      .split("\n")
      .flatMap(_.split("\\[") match {
        case Array(_) => Nil // leaf node, ignore
        case array =>
          array(1).split("\\]") match {
            case Array(left, right) =>
              val geneIndex = left.split("<")(0).substring(1).toInt

              val scores = Scores(
                frequency = 1,
                gain  = getValue("gain", right),
                cover = getValue("cover", right)
              )

              (geneIndex, scores) :: Nil
          }
      })

  private def getValue(key: String, string: String) =
    string.split(",").find(_.startsWith(key)).map(_.split("=")(1)).get.toFloat

}

case class Scores(frequency: Frequency,
                  gain: Gain,
                  cover: Cover) {

  def |+|(that: Scores) = Scores(
    this.frequency + that.frequency,
    this.gain      + that.gain,
    this.cover     + that.cover
  )

}

object Scores {

  val ZERO = Scores(0, 0f, 0f)

}