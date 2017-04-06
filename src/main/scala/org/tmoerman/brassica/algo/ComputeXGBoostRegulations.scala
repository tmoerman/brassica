package org.tmoerman.brassica.algo

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.scala.{Booster, XGBoost}
import org.tmoerman.brassica._
import org.tmoerman.brassica.algo.ComputeXGBoostRegulations._
import org.tmoerman.brassica.util.BreezeUtils.toDMatrix

/**
  * @author Thomas Moerman
  */
case class ComputeXGBoostRegulations(params: XGBoostRegressionParams)
                                    (regulators: List[Gene],
                                     regulatorCSC: CSCMatrix[Expression],
                                     partitionIndex: Int) extends PartitionTask[Regulation] {
  import params._

  private[this] val cachedRegulatorDMatrix = toDMatrix(regulatorCSC)

  /**
    * @return Returns the inferred Regulation instances for one ExpressionByGene instance.
    */
  override def apply(expressionByGene: ExpressionByGene): Iterable[Regulation] = {
    val targetGene = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    println(s"-> target: $targetGene, regulator: $targetIsRegulator, partition: $partitionIndex")

    // remove the target gene column if target gene is a regulator
    val cleanedDMatrixGenesToIndices =
      regulators
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene } // remove the target from the predictors

    // slice the target gene column from the regulator CSC matrix and create a new DMatrix
    val (regulatorDMatrix, disposeAll) =
      if (targetIsRegulator) {
        val cleanRegulatorCSC = regulatorCSC(0 until regulatorCSC.rows, cleanedDMatrixGenesToIndices.map(_._2))
        val cleanRegulatorDMatrix = toDMatrix(cleanRegulatorCSC)

        (cleanRegulatorDMatrix, () => cleanRegulatorDMatrix.delete())
      } else {
        (cachedRegulatorDMatrix, () => Unit)
      }
    regulatorDMatrix.setLabel(expressionByGene.response)

    // train the model
    val booster = XGBoost.train(regulatorDMatrix, boosterParams, nrRounds)
    val result  = toRegulations(targetGene, cleanedDMatrixGenesToIndices, booster)

    // clean up resources
    booster.dispose
    disposeAll()

    result
  }

  override def dispose(): Unit = {
    cachedRegulatorDMatrix.delete()
  }

}

object ComputeXGBoostRegulations {

  def toRegulations(targetGene: Gene,
                    cleanedDMatrixGenesToIndices: List[(Gene, Int)],
                    booster: Booster): Seq[Regulation] = {

    val cleanedDMatrixGenes = cleanedDMatrixGenesToIndices.map(_._1)

    booster
      .getFeatureScore()
      .map { case (feature, score) =>
        val featureIndex = feature.substring(1).toInt
        val regulatorGene = cleanedDMatrixGenes(featureIndex)
        val importance = score.toFloat

        Regulation(regulatorGene, targetGene, importance)
      }
      .toSeq
      .sortBy(-_.importance)
  }

}