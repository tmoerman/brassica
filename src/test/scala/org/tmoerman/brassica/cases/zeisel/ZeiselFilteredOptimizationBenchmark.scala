package org.tmoerman.brassica.cases.zeisel

import java.lang.Runtime.getRuntime

import org.apache.spark.sql.SaveMode.Overwrite
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{ScenicPipeline, XGBoostOptimizationParams, XGBoostSuiteBase}
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.cases.DataReader._

/**
  * @author Thomas Moerman
  */
class ZeiselFilteredOptimizationBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val zeiselFiltered = props("zeiselFiltered")
  val writePath      = props("zeiselFilteredOptimization")
  val mouseTFs       = props("mouseTFs")

  val nrCores = getRuntime.availableProcessors

  val targets = Set("Gad1", "Pkm", "Hapln2", "Dlx1", "Sox10")

  val optimizationParams: XGBoostOptimizationParams =
    XGBoostOptimizationParams(
      nrTrialsPerBatch = 200,
      nrBatches = 88,
      onlyBestTrial = false)

  "Zeisel filtered optimization" should "run" in {
    val expressionsByGene = readText(spark, zeiselFiltered)

    val TFs = readTFs(mouseTFs).toSet

    val optimizedHyperParamsDS =
      ScenicPipeline
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