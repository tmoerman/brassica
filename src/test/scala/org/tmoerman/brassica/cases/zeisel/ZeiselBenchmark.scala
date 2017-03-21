package org.tmoerman.brassica.cases.zeisel

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.zeisel.ZeiselReader.ZEISEL_GENE_COUNT
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.util.{PropsReader, TimeUtils}
import org.tmoerman.brassica.{RegressionParams, ScenicPipeline, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class ZeiselBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.2,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.7,
    "gamma" -> 2
  )

  val params =
    RegressionParams(
      nrRounds = 25,
      boosterParams = boosterParams)

  val zeiselMrna = props("zeisel")

  val mouseTFs = props("mouseTFs")

  "the Zeisel emb.par pipeline from parquet" should "run" in {
    val TFs = ZeiselReader.readTFs(mouseTFs).toSet

    val expressionByGene = ZeiselReader.apply(spark, zeiselMrna)

    val genes = expressionByGene.genes

    val nrTargets = Seq(1, 5, 10, 25, 100, 250, 1000, 2500, 10000, ZEISEL_GENE_COUNT)

    val machine = PropsReader.currentProfile.get

    val durations =
      nrTargets.map { nr =>

        val targets = genes.take(nr)
        val suffix = s"t${targets.size}_r${params.nrRounds}_"
        val out = s"src/test/resources/out/zeisel_$suffix"

        deleteDirectory(new File(out))

        val (_, duration) = TimeUtils.profile {
          val result =

          ScenicPipeline
            .apply(
              expressionByGene,
              candidateRegulators = TFs,
              targets = targets.toSet,
              params = params,
              nrPartitions = Some(spark.sparkContext.defaultParallelism))

          result.repartition(4).write.parquet(out)
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
