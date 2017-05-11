package org.tmoerman.grnboost.algo

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import org.tmoerman.grnboost._
import org.tmoerman.grnboost.util.BreezeUtils._
import InferXGBoostRegulations._
import org.tmoerman.grnboost.util.Elbow

import scala.collection.immutable.Stream.continually

/**
  * @author Thomas Moerman
  */
case class InferXGBoostRegulations(params: XGBoostRegressionParams)
                                  (regulators: List[Gene],
                                   regulatorCSC: CSCMatrix[Expression],
                                   partitionIndex: Int) extends PartitionTask[RawRegulation] {
  import params._

  private[this] val cachedRegulatorDMatrix = toDMatrix(regulatorCSC)

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
      val cleanRegulatorDMatrix = toDMatrix(regulatorCSC dropColumn targetColumnIndex)
      val cleanRegulators = regulators.filterNot(_ == targetGene)

      // set the response labels and train the model
      cleanRegulatorDMatrix.setLabel(expressionByGene.response)

      val result = inferRegulations(targetGene, cleanRegulators, cleanRegulatorDMatrix)

      cleanRegulatorDMatrix.delete()

      result
    } else {
      // set the response labels and train the model
      cachedRegulatorDMatrix.setLabel(expressionByGene.response)

      val result = inferRegulations(targetGene, regulators, cachedRegulatorDMatrix)

      result
    }
  }

  private def inferRegulations(targetGene: Gene,
                               regulators: List[Gene],
                               regulatorDMatrix: DMatrix): Seq[RawRegulation] = {

    val booster = XGBoost.train(regulatorDMatrix, boosterParams.withDefaults, nrRounds)

    val regulations = toRawRegulations(targetGene, regulators, booster, params)

    booster.dispose

    regulations
  }

  /**
    * Dispose the cached DMatrix.
    */
  override def dispose(): Unit = {
    cachedRegulatorDMatrix.delete()
  }

}

object InferXGBoostRegulations {

  type Metrics = (Frequency, Gain, Cover)

  val ZERO = Map[GeneIndex, Metrics]() withDefaultValue (0, 0f, 0f)

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
    import params._

    val boosterModelDump = booster.getModelDump(withStats = true)

    val rawRegulations =
      aggregateBoosterMetrics(boosterModelDump)
        .toSeq
        .map{ case (featureIndex, (freq, gain, cover)) => {
          val regulatorGene = regulators(featureIndex)

          RawRegulation(regulatorGene, targetGene, freq, gain, cover)
        }}
        .sortBy(-_.gain)

    if (elbowCutoff) {
      val gains = rawRegulations.map(_.gain.toDouble)

      val elbows = Elbow(gains, sensitivity = 0.5)

      rawRegulations
        .zip(toElbowStream(elbows))
        .map{ case (reg, e) => if (e.isDefined) reg.copy(elbow = e) else reg }
    } else {
      rawRegulations
    }
  }

  /**
    * @param elbows
    * @return Returns a lazy Stream of Elbow Option instances.
    */
  def toElbowStream(elbows: List[Int]): Stream[Option[Int]] =
    (0 :: elbows.map(_ + 1)) // include the data point at index
      .sliding(2, 1)
      .zipWithIndex
      .flatMap {
        case (a :: b :: Nil, i) => Seq.fill(b-a)(Some(i))
        case _                  => Nil // makes match exhaustive
      }
      .toStream ++ continually(None)

  /**
    * See Python implementation:
    *   https://github.com/dmlc/xgboost/blob/d943720883f0e70ce1fbce809e373908b47bd506/python-package/xgboost/core.py#L1078
    *
    * @param boosterModelDump Trained booster model dump.
    * @return Returns the feature importance metrics parsed from all trees (amount == nr boosting rounds) in the
    *         specified trained booster model.
    */
  def aggregateBoosterMetrics(boosterModelDump: Seq[String]): Map[GeneIndex, Metrics] =
    boosterModelDump
      .flatMap(parseTreeMetrics)
      .foldLeft(ZERO){ case (acc, (geneIndex, (freq, gain, cover))) =>
        val (f, g, c) = acc(geneIndex)
        acc.updated(geneIndex, (f + freq, g + gain, c + cover))
      }

  /**
    * @param treeInfo
    * @return Returns the feature importance metrics parsed from one tree.
    */
  def parseTreeMetrics(treeInfo: String): Array[(GeneIndex, Metrics)] =
    treeInfo
      .split("\n")
      .flatMap(_.split("\\[") match {
        case Array(_) => Nil // leaf node, ignore
        case array =>
          array(1).split("\\]") match {
            case Array(left, right) =>
              val geneIndex = left.split("<")(0).substring(1).toInt

              val stats = right.split(",")

              val freq  = 1
              val gain  = stats.find(_.startsWith("gain")).map(_.split("=")(1)).get.toFloat
              val cover = stats.find(_.startsWith("cover")).map(_.split("=")(1)).get.toFloat

              (geneIndex, (freq, gain, cover)) :: Nil
          }
      })

}