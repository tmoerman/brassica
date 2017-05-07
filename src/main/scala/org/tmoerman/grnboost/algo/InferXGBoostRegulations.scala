package org.tmoerman.grnboost.algo

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import org.tmoerman.grnboost._
import org.tmoerman.grnboost.util.BreezeUtils._
import InferXGBoostRegulations._

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

    val boosters = trainBoosters(regulatorDMatrix)

    val regulations = toRawRegulations(targetGene, regulators, boosters)

    boosters.foreach(_.dispose)

    regulations
  }

  private def trainBoosters(regulatorDMatrix: DMatrix) =
    if (ensembleSize > 1) {
      val seed = boosterParams.get("seed").map(_.toString.toInt).getOrElse(DEFAULT_SEED)
      val rng = random(seed)

      Seq.tabulate(ensembleSize)(_ =>
        XGBoost.train(regulatorDMatrix, boosterParams.withDefaults.withSeed(rng.nextInt), nrRounds))
    } else {
      Seq(
        XGBoost.train(regulatorDMatrix, boosterParams.withDefaults, nrRounds))
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
    * @param boosters
    * @return Returns the raw scores for regulation.
    */
  def toRawRegulations(targetGene: Gene,
                       regulators: List[Gene],
                       boosters: Seq[Booster]): Seq[RawRegulation] = {

    val boosterModelDumps = boosters.flatMap(_.getModelDump(withStats = true))

    aggregateBoosterMetrics(boosterModelDumps)
      .map{ case (featureIndex, (freq, gain, cover)) => {
        val regulatorGene = regulators(featureIndex)

        RawRegulation(regulatorGene, targetGene, freq, gain, cover)
      }}
      .toSeq
      .sortBy(r => -(r.gain / r.frequency))
  }

  /**
    * @param targetGene The target gene.
    * @param regulators The regulator gene names.
    * @param boosters The booster instances.
    * @param metric The feature importance metric.
    * @return Returns the Regulations in function of specified metric, extracted from the specified trained booster model.
    */
  @deprecated def toRegulations(targetGene: Gene,
                                regulators: List[Gene],
                                boosters: Seq[Booster],
                                metric: FeatureImportanceMetric): Seq[Regulation] = {

    val boosterModelDumps = boosters.flatMap(_.getModelDump(withStats = true))

    aggregateBoosterMetrics(boosterModelDumps)
      .map{ case (featureIndex, (freq, gain, cover)) => {
        val regulatorGene = regulators(featureIndex)

        val importance = metric match {
          case FREQ  => freq.toFloat
          case GAIN  => gain
          case COVER => cover
        }

        Regulation(regulatorGene, targetGene, importance)
      }}
      .toSeq
      .sortBy(-_.importance)
  }

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