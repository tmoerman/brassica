package org.tmoerman.brassica.cases.dream5

import java.io.File

import org.apache.commons.io.FileUtils.deleteDirectory
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.{Gene, ScenicPipelineOld, XGBoostSuiteBase}

/**
  * @author Thomas Moerman
  */
class Dream5PipelineSpec extends FlatSpec with XGBoostSuiteBase with Matchers {

  behavior of "ScenicPipeline"

  it should "run on 5 targets of the ecoli data set" in {
    val (df, genes) = Dream5Reader.readOriginalData(spark, ecoliData, ecoliGenes)

    val TFs = Dream5Reader.readTFs(ecoliTFs).toSet

    val (grn, info) =
      ScenicPipelineOld.apply(
        spark,
        df,
        genes,
        nrRounds =  10,
        candidateRegulators = TFs,
        targets = genes.take(5).toSet)

    grn.show(5)

    println(info.mkString("\n"))

    val out = props("out") + "ecoli_1_target_2.csv"

    deleteDirectory(new File(out))

    grn.coalesce(1).write.csv(out)
  }

  it should "run on s. aureus" in {

  }

  it should "run on s. cerevisiae" in {

  }

}