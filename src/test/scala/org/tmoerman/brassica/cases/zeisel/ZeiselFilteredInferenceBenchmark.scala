package org.tmoerman.brassica.cases.zeisel

import java.io.File

import org.apache.commons.io.FileUtils._
import org.apache.spark.sql.SaveMode.Overwrite
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.DataReader._
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.util.{PropsReader, TimeUtils}
import org.tmoerman.brassica.{ScenicPipeline, XGBoostRegressionParams, GRNBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class ZeiselFilteredInferenceBenchmark extends FlatSpec with GRNBoostSuiteBase with Matchers {

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.15,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.7,
    "gamma" -> 2,
    "nthread" -> 1
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 50,
      boosterParams = boosterParams)

  val zeiselFiltered = props("zeiselFiltered")

  val mouseTFs = props("mouseTFs")

  "the (filtered, cfr. Sara) Zeisel emb.par pipeline from parquet" should "run" in {
    val TFs = readTFs(mouseTFs).toSet

    val ds = readText(spark, zeiselFiltered)

    val genes = toGenes(spark, ds)

    val nrTargets = Seq(genes.size) // Seq(1, 5, 10, 25, 100, 250, 1000, 2500, 10000, genes.size)
    // val nrTargets = Nil // all

    val machine = PropsReader.currentProfile.get

    val durations =
      nrTargets.map { nr =>

        val targets = genes.take(nr)
        val suffix = s"t${targets.size}_r${params.nrRounds}_"
        val out = s"src/test/resources/out/zeisel_filtered_$suffix"

        deleteDirectory(new File(out))

        val (_, duration) = TimeUtils.profile {
          val expressionByGene = readText(spark, zeiselFiltered)

          val result =
            ScenicPipeline
              .inferRegulations(
                expressionByGene,
                candidateRegulators = TFs,
                targets = targets.toSet,
                params = params,
                nrPartitions = Some(spark.sparkContext.defaultParallelism))
              .repartition(1)
              .persist

          result.write.mode(Overwrite).parquet(s"${out}_parquet")
          result.write.mode(Overwrite).csv(s"${out}_csv")
        }

        s"| $machine | ${targets.size} | ${params.nrRounds} | ${duration.toSeconds} |"
      }

    val table =
      "| machine | # targets | # boosting rounds | duration seconds |" ::
      "| ---     | ---:      | ---:              | ---:             |" ::
      durations.toList

    println(table.mkString("\n"))
  }

}
