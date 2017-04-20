package org.tmoerman.grnboost.algo

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.scala.{Booster, XGBoost}
import org.tmoerman.grnboost._
import org.tmoerman.grnboost.util.BreezeUtils._

/**
  * @author Thomas Moerman
  */
case class InferXGBoostRegulations(params: XGBoostRegressionParams)
                                  (regulators: List[Gene],
                                   regulatorCSC: CSCMatrix[Expression],
                                   partitionIndex: Int) extends PartitionTask[Regulation] {
  import params._

  private[this] val cachedRegulatorDMatrix = toDMatrix(regulatorCSC)

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
      val cleanRegulatorDMatrix = toDMatrix(regulatorCSC dropColumn targetColumnIndex)

      // set the response labels and train the model
      cleanRegulatorDMatrix.setLabel(expressionByGene.response)
      val booster = XGBoost.train(cleanRegulatorDMatrix, boosterParams.withDefaults, nrRounds)
      val result  = toRegulations(targetGene, regulators.filterNot(_ == targetGene), booster)

      // clean up resources
      booster.dispose
      cleanRegulatorDMatrix.delete()

      result
    } else {
      // set the response labels and train the model
      cachedRegulatorDMatrix.setLabel(expressionByGene.response)
      val booster = XGBoost.train(cachedRegulatorDMatrix, boosterParams.withDefaults, nrRounds)
      val result  = toRegulations(targetGene, regulators, booster)

      // clean up resources
      booster.dispose

      result
    }
  }

  /**
    * @return Returns a Regulation instance.
    */
  def toRegulations(targetGene: Gene, regulators: List[Gene], booster: Booster): Seq[Regulation] = {
    booster
      .getFeatureScore()
      .map { case (feature, score) =>
        val featureIndex  = feature.substring(1).toInt
        val regulatorGene = regulators(featureIndex)
        val importance    = score.toFloat

        Regulation(regulatorGene, targetGene, importance)
      }
      .toSeq
      .sortBy(-_.importance)
  }

  /**
    * Dispose the cached DMatrix.
    */
  override def dispose(): Unit = {
    cachedRegulatorDMatrix.delete()
  }

}