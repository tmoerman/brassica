package org.tmoerman.brassica.cases.zeisel

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{RegressionParams, XGBoostSuiteBase}
import org.tmoerman.brassica.util.{PropsReader, TimeUtils}
import org.tmoerman.brassica.util.TimeUtils.pretty

/**
  * @author Thomas Moerman
  */
class ZeiselBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val params =
    RegressionParams(
      normalize = false,
      nrRounds = 10,
      boosterParams = Map(
        "seed" -> 777,
        "silent" -> 1
      ))

  "the emb.par pipeline from parquet" should "run" in {
    val TFs = ZeiselReader.readTFs(mouseTFs).toSet

    val genes = ZeiselReader.readGenes(spark, zeiselMrna)

    val nrTargets = Seq(1, 5, 10, 25, 100, 250, 1000, 2500, 10000, genes.size).take(5)

    val machine = PropsReader.currentProfile

    val durations =
      nrTargets.map { nr =>

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
                nrPartitions = Some(spark.sparkContext.defaultParallelism))

          result.write.parquet(out)

          result
        }

        s"| $machine | ${targets.size} | ${params.nrRounds} | ${pretty(duration)} | $params |"
      }

    val table =
      "| machine | # targets | # boosting rounds | duration seconds | params |" ::
      "| ---     | ---:      | ---:              | ---:             | ---    |" ::
      durations.toList

    println(table.mkString("\n"))
  }

}
