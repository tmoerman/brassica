package org.tmoerman.brassica.cases.zeisel

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{ScenicPipeline_OLD, Gene, XGBoostSuiteBase}
import org.tmoerman.brassica.util.PropsReader.props

/**
  * @author Thomas Moerman
  */
class ZeiselPipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline"

  val params = Map(
    // "alpha" -> 10, // L1 regularization, cfr. Lasso
    // "colsample_bytree" -> 0.5f,
    // "subsample" -> 0.5f,
    "seed" -> 777,
    "silent" -> 1
  )

  it should "run the embarrassingly parallel pipeline" in {

    // FIXME finish this !!!

//    val result =
//      ZeiselPipeline
//        .apply(
//          spark,
//          params,
//        )

  }

  it should "run the old Spark scenic pipeline" in {
    val (df, genes) = ZeiselReader.fromParquet(spark, zeiselParquet, zeiselMrna)

    val TFs: List[Gene] = ZeiselReader.readTFs(mouseTFs)

    val (grn, info) =
      ScenicPipeline_OLD.apply(
        spark,
        df,
        genes,
        params = params,
        nrRounds = 10,
        candidateRegulators = TFs,
        //targets = List("Gad1"),
        targets = List("Gad1", "Celf6", "Dlx1", "Dlx2", "Peg3", "Rufy3", "Msi2", "Maf", "Sp8", "Sox1", "Sp9"),
        nrWorkers = Some(8)
      )

    grn.show()

    println(info.mkString("\n"))

    val out = props("out") + "zeisel"

    deleteDirectory(new File(out))

    grn.coalesce(1).write.csv(out)
  }

}