package org.tmoerman.brassica.cases.zeisel

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.{ScenicPipeline, Gene, XGBoostSuiteBase}
import org.tmoerman.brassica.util.PropsReader.props

/**
  * @author Thomas Moerman
  */
class ZeiselPipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "Scenic pipeline"

  val zeiselMrna    = props("zeisel")
  val zeiselParquet = props("zeiselParquet")

  val mouseTFs = props("mouseTFs")

  val params = Map(
    // "alpha" -> 10, // L1 regularization, cfr. Lasso
    //"colsample_bytree" -> 0.5f,
    "seed" -> 666,
    "silent" -> 1
  )

  it should "run on target subset of the Zeisel data set" in {
    val (df, genes) = ZeiselReader.fromParquet(spark, zeiselParquet, zeiselMrna)

    val TFs: List[Gene] = ZeiselReader.readTFs(mouseTFs)

    val (grn, info) =
      ScenicPipeline.apply(
        spark,
        df,
        genes,
        params = params,
        nrRounds = 10000,
        candidateRegulators = TFs,
        targets = genes.take(1))

    grn.show(50)

    println(info.mkString("\n"))

    val out = props("out") + "zeisel"

    deleteDirectory(new File(out))

    grn.coalesce(1).write.csv(out)
  }

}