package org.tmoerman.brassica.cases.dream5

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.{Gene, ScenicPipeline, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class Dream5PipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "ScenicPipeline"

  it should "run on ecoli with one candidate regulator" in {
    val (df, genes) = Dream5Reader(spark, ecoliData, ecoliGenes)

    val TFs: List[Gene] = Dream5Reader.TFs(ecoliTFs)

    val targets = List("aaaD", "aaeA", "aaeB", "aaeR", "aaeX")

    val (grn, stats) =
      ScenicPipeline.apply(
        spark,
        df,
        genes,
        nrRounds =  10,
        candidateRegulators = TFs,
        targets = targets)

    grn.show(5)

    println(stats.mkString("\n"))

    grn.coalesce(1).write.csv(props("out") + "ecoli_1_target_2.csv")
  }

  it should "run on s. aureus" in {

  }

  it should "run on s. cerevisiae" in {

  }

}