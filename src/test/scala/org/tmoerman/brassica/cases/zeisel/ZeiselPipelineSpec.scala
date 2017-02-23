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

  it should "run on 5 targets of the Zeisel data set" in {
    val (df, genes) = ZeiselReader.fromParquet(spark, zeiselParquet, zeiselMrna)

    val TFs: List[Gene] = ZeiselReader.TFs(mouseTFs)

    val (grn, info) =
      ScenicPipeline.apply(
        spark,
        df,
        genes,
        nrRounds = 10,
        candidateRegulators = TFs,
        targets = genes.take(5))

    grn.show(5)

    println(info.mkString("\n"))

    val out = props("out") + "zeisel"

    deleteDirectory(new File(out))

    grn.coalesce(1).write.csv(out)
  }

}