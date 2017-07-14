package org.aertslab.grnboost.cases.zeisel

import java.lang.Runtime.getRuntime

import org.apache.spark.sql.SaveMode.Overwrite
import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost.{GRNBoost, GRNBoostSuiteBase, XGBoostOptimizationParams}
import org.aertslab.grnboost.util.PropsReader.props
import org.aertslab.grnboost.DataReader._
import org.aertslab.grnboost.Specs.Server

/**
  * @author Thomas Moerman
  */
class ZeiselFilteredOptimizationBenchmark extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val zeiselFiltered = props("zeiselFiltered")
  val writePath      = props("zeiselFilteredOptimization")
  val mouseTFs       = props("mouseTFs")

  val nrCores = getRuntime.availableProcessors

  val targets = Set("Gad1", "Pkm", "Hapln2", "Dlx1", "Sox10")

  val optimizationParams: XGBoostOptimizationParams =
    XGBoostOptimizationParams(
      nrTrials = 200,
      // nrBatches = 88,
      onlyBestTrial = false)

  "Zeisel filtered optimization" should "run" taggedAs Server ignore {
    val expressionsByGene = readExpressionsByGene(spark, zeiselFiltered)

    val TFs = readRegulators(mouseTFs).toSet

    val optimizedHyperParamsDS =
      GRNBoost
        .optimizeHyperParams(
          expressionsByGene,
          candidateRegulators = TFs,
          params = optimizationParams,
          targets = targets,
          nrPartitions = Some(nrCores))
        .cache()

    optimizedHyperParamsDS
      .write
      .mode(Overwrite)
      .parquet(writePath)
  }

}