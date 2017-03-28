package org.tmoerman

import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.{SparseVector, Vector => MLVector}
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.Dataset

import org.apache.spark.ml.linalg.BreezeMLConversions._

/**
  * Constants, case classes and type aliases.
  *
  * @author Thomas Moerman
  */
package object brassica {

  type Path = String

  type Count = Int
  type Index = Long
  type Gene  = String
  type BoosterParams = Map[String, Any]

  type CellIndex = Int
  type CellCount = Int
  type GeneIndex = Int
  type GeneCount = Int
  type Expression = Float
  type Importance = Float

  val VALUES      = "values"
  val GENE        = "gene"
  val EXPRESSION  = "expression"
  val REGULATORS  = "regulators"
  val TARGET_GENE = "target_gene"

  val TARGET_INDEX    = "target_index"
  val TARGET_NAME     = "target_name"
  val REGULATOR_INDEX = "regulator_index"
  val REGULATOR_NAME  = "regulator_name"
  val IMPORTANCE      = "importance"

  val DEFAULT_BOOSTER_PARAMS: BoosterParams = Map(
    "silent" -> 1
  )

  /**
    * @param gene The gene name.
    * @param values The sparse expression vector.
    */
  case class ExpressionByGene(gene: Gene, values: MLVector) { // TODO rename values -> "expression"
    def response: Array[Float] = values.toArray.map(_.toFloat)
  }

  private[this] val normalizer = new Normalizer(p = 2.0)

  /**
    * Implicit pimp class for adding functions to Dataset[ExpressionByGene]
    * @param ds The Dataset to pimp.
    */
  implicit class ExpressionByGeneFunctions(val ds: Dataset[ExpressionByGene]) {
    import ds.sparkSession.implicits._

    /**
      * @return Returns the genes in the Dataset as List of Strings.
      */
    def genes: List[Gene] = ds.select($"gene").rdd.map(_.getString(0)).collect.toList

    /**
      * @return Returns the Dataset, with normalized (p=2) expression vectors.
      */
    @deprecated("unclear whether useful") def normalized: Dataset[ExpressionByGene] =
      ds.map(e => e.copy(values = normalizer.transform(OldVectors.fromML(e.values)).asML))

    def standardized: Dataset[ExpressionByGene] =
      ds.map(e => {

        import breeze.stats._

        val v = e.values.br

        val mu = mean.apply(v)
        val s  = stddev.apply(v)

        val result = (v - mu) / s

        e.copy(values = result.ml)
      })

    /**
      * @param cellIndices
      * @return Returns the Dataset with values sliced in function of the specified Seq of cell indices.
      */
    def slice(cellIndices: Seq[CellIndex]): Dataset[ExpressionByGene] = {
      val slicer =
        new VectorSlicer()
          .setInputCol("values")
          .setOutputCol("sliced")
          .setIndices(cellIndices.toArray)

      Some(ds)
        .map(slicer.transform)
        .map(_.select($"gene", $"sliced"))
        .map(_.withColumnRenamed("sliced", "values"))
        .map(_.as[ExpressionByGene])
        .get
    }

  }

  /**
    * @param regulator The regulator gene name.
    * @param target The target gene name.
    * @param importance The inferred importance of the regulator vis-a-vis the target.
    */
  case class Regulation(regulator: Gene, target: Gene, importance: Importance)

  val DEFAULT_NR_BOOSTING_ROUNDS = 50
  val DEFAULT_NR_FOLDS = 10

  /**
    * @param boosterParams The XGBoost Map of booster parameters.
    * @param nrRounds The nr of boosting rounds.
    * @param nrFolds The nr of cross validation folds.
    * @param normalizeImportances Normalize the Regulation importance scores (probably useless).
    */
  case class RegressionParams(boosterParams: BoosterParams = DEFAULT_BOOSTER_PARAMS,
                              nrRounds: Int = DEFAULT_NR_BOOSTING_ROUNDS,
                              nrFolds: Int = DEFAULT_NR_FOLDS,
                              normalizeImportances: Boolean = false)

}