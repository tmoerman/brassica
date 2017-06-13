package org.aertslab.grnboost.algo

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import org.aertslab.grnboost._
import org.aertslab.grnboost.util.BreezeUtils._
import CalculateLossByRound._

/**
  * @author Thomas Moerman
  */
case class CalculateLossByRound(params: XGBoostRegressionParams)
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

      val result = calculateLossByRound(params, targetGene, cleanRegulators, cleanRegulatorDMatrix)

      cleanRegulatorDMatrix.delete()

      result
    } else {
      // set the response labels and train the model
      cachedRegulatorDMatrix.setLabel(expressionByGene.response)

      val result = calculateLossByRound(params, targetGene, regulators, cachedRegulatorDMatrix)

      result
    }
  }

  override def dispose(): Unit = {
    cachedRegulatorDMatrix.delete()
  }

}

object CalculateLossByRound {

  def calculateLossByRound(params: XGBoostRegressionParams,
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

}