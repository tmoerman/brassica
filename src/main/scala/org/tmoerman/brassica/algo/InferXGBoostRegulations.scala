package org.tmoerman.brassica.algo

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.scala.{Booster, XGBoost}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, sum}
import org.apache.spark.sql.types.DataTypes.FloatType
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.tmoerman.brassica._
import org.tmoerman.brassica.algo.InferXGBoostRegulations._
import org.tmoerman.brassica.util.BreezeUtils.toDMatrix

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
    val booster = XGBoost.train(regulatorDMatrix, boosterParams.withDefaults, nrRounds)
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

/**
  * Companion object exposing stateless functions.
  */
object InferXGBoostRegulations {

  /**
    * @return Returns a Regulation instance.
    */
  def toRegulations(targetGene: Gene,
                    cleanedDMatrixGenesToIndices: List[(Gene, Int)],
                    booster: Booster): Seq[Regulation] = {

    val cleanedDMatrixGenes = cleanedDMatrixGenesToIndices.map(_._1)

    booster
      .getFeatureScore()
      .map { case (feature, score) =>
        val featureIndex  = feature.substring(1).toInt
        val regulatorGene = cleanedDMatrixGenes(featureIndex)
        val importance    = score.toFloat

        Regulation(regulatorGene, targetGene, importance)
      }
      .toSeq
      .sortBy(-_.importance)
  }

  /**
    * @param ds The Dataset of Regulations.
    * @return Returns a Dataset where the Regulation have been normalized by dividing the importance scores by the sum
    *         of importance scores per target.
    */
  def normalizeBySum(ds: Dataset[Regulation]): Dataset[Regulation] = {
    import ds.sparkSession.implicits._

    val aggImportanceByTarget =
      ds
        .groupBy($"target")
        .agg(sum($"importance").as("agg_importance"))

    ds
      .join(aggImportanceByTarget, ds("target") === aggImportanceByTarget("target"), "inner")
      .withColumn("normalized_importance", $"importance" / $"agg_importance")
      .select(ds("regulator"), ds("target"), $"normalized_importance".as("importance").cast(FloatType))
      .as[Regulation]
  }

  /**
    * @param maxRank
    * @param ds
    * @return Returns a Dataset of Regulations, ordered by rank per target.
    */
  @deprecated("experimental")
  def orderByRank(maxRank: Double = 50d)(ds: Dataset[Regulation]): Dataset[Regulation] = {
    import ds.sparkSession.implicits._

    val w = Window.partitionBy($"target").orderBy($"importance".desc)

    ds
      .withColumn("rank", rank.over(w))
      .withColumn("normalized_importance", (-$"rank" + 1 + maxRank) / maxRank)
      .sort($"normalized_importance".desc)
      .select(ds("regulator"), ds("target"), $"importance".cast(FloatType))
      .as[Regulation]
  }

  /**
    * @param top The maximum amount of regulations to keep.
    * @param ds The Dataset of Regulation instances.
    * @return Returns the truncated Dataset.
    */
  def keepTop(top: Int = 100000)(ds: Dataset[Regulation]): Dataset[Regulation] = {
    import ds.sparkSession.implicits._

    ds
      .sort($"importance".desc)
      .rdd
      .zipWithIndex
      .filter(_._2 < top)
      .keys
      .toDS
  }

  /**
    * Repartition to 1 and save to a single file.
    */
  def saveTxt(path: String)(ds: Dataset[Regulation]): Unit =
    ds
      .rdd
      .map(_.productIterator.mkString("\t"))
      .repartition(1)
      .saveAsTextFile(path)

}