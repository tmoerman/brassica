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
    // "alpha" -> 10, // L1 regularization, cfr. Lasso
    // "colsample_bytree" -> 0.5f,
    // "subsample" -> 0.5f,
    "seed" -> 777,
    "silent" -> 1
  )

  val params =
    RegressionParams(
      normalize = false,
      nrRounds = 10,
      boosterParams = boosterParams)

  it should "run the embarrassingly parallel pipeline from raw" in {
    val TFs = ZeiselReader.readTFs(mouseTFs).toSet

    val result =
      ZeiselPipeline
        .apply(
          spark,
          zeiselMrna,
          candidateRegulators = TFs,
          targets = Set("Gad1"),
          params = params,
          cellTop = None,
          nrPartitions = None)

    result.show
  }

  it should "run the emb.par pipeline from parquet" in {
    val TFs = ZeiselReader.readTFs(mouseTFs).toSet

    val genes = ZeiselReader.readGenes(spark, zeiselMrna)

    val nrTargets = Seq(1, 10, 100, 500, 1000, 2500, 10000, genes.size)

    val machine = "giorgio"

    nrTargets
      .foreach { nr =>

        val targets = genes.take(nr)
        val suffix = s"${targets.head}_${targets.size}"
        val out = s"src/test/resources/out/zeisel_GRN_$suffix"

        deleteDirectory(new File(out))

        val (df, duration) = TimeUtils.profile {
          val result =
            ZeiselPipeline
              .fromParquet(
                spark,
                zeiselParquet,
                zeiselMrna,
                candidateRegulators = TFs,
                targets = targets.toSet,
                params = params,
                cellTop = None,
                nrPartitions = None)

          result.write.parquet(out)

          result
        }

        println(s"| $machine | ${targets.size} ")

        println(s"nr targets: ${targets.size}, duration: ${duration.toSeconds} seconds")
      }
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
        nrRounds = 10,
        candidateRegulators = TFs,
        params = boosterParams,
        targets = Set("Gad1"),
        nrWorkers = Some(8))

    grn.show()

    println(info.mkString("\n"))

    val out = props("out") + "zeisel"

    deleteDirectory(new File(out))

    grn.coalesce(1).write.csv(out)
  }

}