package org.tmoerman.brassica.cases.dream5

import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}
import org.tmoerman.brassica.util.PropsReader
import org.tmoerman.brassica.{XGBoostRegressionParams, XGBoostSuiteBase, _}

/**
  * @author Thomas Moerman
  */
class Dream5NetworksBenchmark extends FlatSpec with XGBoostSuiteBase with Matchers {

  val boosterParamsBAK = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.1,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.8,
    "min_child_weight" -> 4,
    "max_depth" -> 4,
    "gamma" -> 2
    //"alpha" -> 7
    //"lambda" -> 7
  )

  val boosterParams = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.2,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.8
  )

  val boosterParamsLOLZ = Map(
    "seed" -> 777,
    "silent" -> 1,
    "eta" -> 0.15,
    "subsample" -> 0.8,
    "colsample_bytree" -> 0.8,
    "min_child_weight" -> 6,
    "max_depth" -> 6
  )

  val params =
    XGBoostRegressionParams(
      nrRounds = 50,
      boosterParams = boosterParamsLOLZ)

  "Dream5 regulation inference" should "run" ignore {
    Seq(1, 3, 4).foreach(computeNetwork)
    // Seq(3).foreach(computeNetwork)
  }

  private def computeNetwork(idx: Int): Unit = {

    println(s"computing network $idx")

    val (dataFile, tfFile) = network(idx)

    val (expressionByGene, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val parquet = s"${PropsReader.props("dream5Out")}/LOLz/Network$idx/"
    // deleteDirectory(new File(parquet))

    val regulationDS =
      ScenicPipeline
        .inferRegulations(
          expressionByGene,
          candidateRegulators = tfs.toSet,
          params = params,
          // targets = Set("G3"),
          nrPartitions = Some(spark.sparkContext.defaultParallelism))
        .cache()

    regulationDS
      .write
      .mode(Overwrite)
      .parquet(parquet)

//    result
//      .sort($"importance".desc)
//      .rdd
//      .zipWithIndex.filter(_._2 < 100000).keys // top 100K
//      .repartition(1)
//      .map(_.productIterator.mkString("\t"))
//      .saveAsTextFile(out)
  }

  "Normalizing Dream5 networks" should "work" in {
    Seq(1, 3, 4).foreach(normalizeNetwork)
    // Seq(3).foreach(normalizeNetwork)
  }

  private def normalizeNetwork(idx: Int): Unit = {
    import spark.implicits._

    val parquet = s"${PropsReader.props("dream5Out")}/LOLz/Network$idx/"

    val txt = s"${PropsReader.props("dream5Out")}/LOLz/Network${idx}norm/"

    val ds =
      spark
        .read
        .parquet(parquet)
        .as[Regulation]
        .cache

    val sumImportanceByTarget =
      ds
      .groupBy($"target")
      .agg(sum($"importance").as("sum_importance"))

    ds
      .join(sumImportanceByTarget, ds("target") === sumImportanceByTarget("target"), "inner")
      .withColumn("norm_importance", $"importance" / $"sum_importance")
      .sort($"norm_importance".desc)
      .rdd
      .zipWithIndex
      .filter(_._2 <= 10000).keys // keep top 100k regulations
      .map(row => {
        val regulator  = row.getAs[String]("regulator")
        val target     = row.getAs[String]("target")
        val normalized = row.getAs[Double]("norm_importance")

        s"$regulator\t$target\t$normalized"
      })
      .repartition(1)
      .saveAsTextFile(txt)
  }

}