package org.tmoerman.brassica.cases.zeisel

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica._
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.util.TimeUtils

/**
  * @author Thomas Moerman
  */
class ZeiselPipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline on Zeisel"

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1
  )

  val params =
    RegressionParams(
      normalize = false,
      nrRounds = 100,
      boosterParams = boosterParams)

  it should "run the embarrassingly parallel pipeline from raw" in {
    val TFs = ZeiselReader.readTFs(mouseTFs).toSet

    val result =
      ZeiselPipeline
        .apply(
          spark,
          zeiselMrna,
          candidateRegulators = TFs,
          targets = Set("Hapln2"),
          params = params,
          cellTop = None,
          nrPartitions = None)

    println(params)

    result.show
  }

  it should "run the emb.par pipeline from parquet" in {
    val TFs = ZeiselReader.readTFs(mouseTFs).toSet

    val result =
      ZeiselPipeline
        .fromParquet(
          spark,
          zeiselParquet,
          zeiselMrna,
          candidateRegulators = TFs,
          targets = Set("Hapln2"),
          params = params,
          cellTop = None,
          nrPartitions = None)

    println(params)

    result.show
  }

  it should "inspect the written GRN" in {
    val df = spark.read.parquet("src/test/resources/out/zeisel_GRN_Tspan12_100")

    println(df.count)

    df.repartition(1).write.csv("src/test/resources/out/zeisel_GRN_Tspan12_100_CSV")
  }

  it should "run the old Spark scenic pipeline" in {
    val (df, genes) = ZeiselReader.fromParquet(spark, zeiselParquet, zeiselMrna)

    val TFs = ZeiselReader.readTFs(mouseTFs).toSet

    val (grn, info) =
      ScenicPipeline_OLD.apply(
        spark,
        df,
        genes,
        nrRounds = 100,
        candidateRegulators = TFs,
        params = boosterParams,
        targets = Set("Hapln2"),
        nrWorkers = Some(8))

    grn.show()

    println(info.mkString("\n"))

    // val out = props("out") + "zeisel"

    // deleteDirectory(new File(out))

    // grn.coalesce(1).write.csv(out)
  }

}