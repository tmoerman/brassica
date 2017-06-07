package org.aertslab.grnboost.cases.megacell

import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost.DataReader.readRegulators
import org.aertslab.grnboost.util.PropsReader.props
import org.aertslab.grnboost.{CellIndex, ExpressionByGene, GRNBoost, GRNBoostSuiteBase, XGBoostRegressionParams, randomSubset}

import scala.io.Source

/**
  * @author Thomas Moerman
  */
class MegacellPipelineSpec extends FlatSpec with GRNBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline on Megacell"

  val boosterParams = Map(
    "seed" -> 777,
    "eta"              -> 0.01,
    "subsample"        -> 0.8,
    "colsample_bytree" -> 0.25,
    "max_depth"        -> 3,
    "silent" -> 1
    //"gamma" -> 10
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 5000,
      boosterParams = boosterParams)

  val parquet = "src/test/resources/parquet/megacell"

  val mouseTFs = props("mouseTFs")

//  val megacellSubSet3k = props("megacellSubSet3k")
//  val megacellSubSet10k = props("megacellSubSet10k")
//  val megacellSubSet100k = props("megacellSubSet100k")

  it should "meh" in {
    import spark.implicits._

    val TFs = readRegulators(mouseTFs).toSet

    val cellIndicesSubSet: Seq[CellIndex] = randomSubset(3000, 0 until 1300000)
      // Source.fromFile(megacellSubSet3k).getLines.filterNot(_.isEmpty).map(_.trim.toInt).toSeq

    val ds = spark.read.parquet(parquet).as[ExpressionByGene].slice(cellIndicesSubSet).cache

    println(ds.count)

    val result =
      GRNBoost
        .inferRegulations(
          ds,
          candidateRegulators = TFs,
          targets = Set("Lamp3"),
          params = params)

    println(params)

    result
      .sort($"gain".desc)
      .show
  }

}