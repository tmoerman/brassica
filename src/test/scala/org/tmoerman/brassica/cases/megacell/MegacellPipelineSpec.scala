package org.tmoerman.brassica.cases.megacell

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{ScenicPipeline, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class MegacellPipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline"

  val params = Map(
    "seed" -> 777,
    "silent" -> 1
  )

  it should "run on target subset of the Megacell data set" in {
    val (df, genes) = MegacellReader.apply(spark, megacell).get // TODO read Parquet better ???

    val TFs = MegacellReader.readTFs(mouseTFs)

    val (grn, info) =
      ScenicPipeline.apply(
        spark,
        df,
        genes,
        params = params,
        nrRounds = 10,
        candidateRegulators = TFs,
        targets = List("Xkr4", "Rp1", "Sox17", "Lypla1", "Oprk1", "St18"),
        nrWorkers = Some(1)
      )

    grn.show()

    println(info.mkString("\n"))
  }

}