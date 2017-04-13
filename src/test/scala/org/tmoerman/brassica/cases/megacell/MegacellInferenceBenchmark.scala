package org.tmoerman.brassica.cases.megacell

import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.cases.zeisel.ZeiselFilteredReader
import org.tmoerman.brassica.util.PropsReader.props
import org.tmoerman.brassica.{ExpressionByGene, XGBoostRegressionParams, XGBoostSuiteBase, _}

/**
  * @author Thomas Moerman
  */
class MegacellInferenceBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val boosterParamsBio = Map(
    "seed" -> 777,
    "eta" -> 0.15,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.8,
    "min_child_weight" -> 6,
    "max_depth" -> 6
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 50,
      boosterParams = boosterParamsBio)

  val full = props("megacellFull")
  val out  = props("megacellOut")

  "Megacell Gene Network Inference" should "work on 10K cells" in {
    inferSub(10000)
  }

  private def inferSub(nrCells: CellCount) = {
    import spark.implicits._

    val TFs = ZeiselFilteredReader.readTFs(mouseTFs).toSet

    val ds = spark.read.parquet(full).as[ExpressionByGene]

    val totalCellCount = ds.head.values.size

    val subset_10k = randomSubset(nrCells, 0 until totalCellCount)

    val sliced = ds.slice(subset_10k)

    val regulationsDS =
      ScenicPipeline
        .inferRegulations(
          sliced,
          candidateRegulators = TFs,
          params = params,
          nrPartitions = Some(spark.sparkContext.defaultParallelism))
        .cache()
    
    regulationsDS
      .write
      .parquet(out + s"subset_$nrCells")
  }
}