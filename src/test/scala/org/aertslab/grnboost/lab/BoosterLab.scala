package org.aertslab.grnboost.lab

import java.io.ByteArrayInputStream

import ml.dmlc.xgboost4j.java.XGBoostUtils._
import ml.dmlc.xgboost4j.scala.XGBoostConversions._
import org.aertslab.grnboost.algo.InferXGBoostRegulations
import org.aertslab.grnboost.cases.dream5.{Dream5Reader, network}
import org.aertslab.grnboost.util.BreezeUtils._
import org.aertslab.grnboost.{GRNBoost, GRNBoostSuiteBase, XGBoostRegressionParams, _}
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers, Suite}

/**
  * @author Thomas Moerman
  */
class BoosterLab extends FlatSpec with GRNBoostSuiteBase with Matchers { self: Suite =>

  override def conf: SparkConf =
    super
      .conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.aertslab.grnboost.kryo.GRNBoostKryoRegistrator")

  behavior of "serializing Booster models"

  it should "work" in {

    val (dataFile, tfFile) = network(3)

    val (ds, tfs) = Dream5Reader.readTrainingData(spark, dataFile, tfFile)

    val candidateRegulators = tfs.toSet

    val regulators = ds.genes.filter(candidateRegulators.contains)

    val genes = ds.genes

    val regulatorCSC = GRNBoost.reduceToRegulatorCSCMatrix(ds, regulators)

    println(
      s"""
        |* regulators : ${candidateRegulators.size}
        |* csc.rows   : ${regulatorCSC.rows}
        |* csc.cols   : ${regulatorCSC.cols}
      """.stripMargin)

    val boosterParams = Map(
      "seed"      -> 777,
      "eta"       -> 0.1,
      "max_depth" -> 2,
      "silent"    -> 1
    )

    val params =
      XGBoostRegressionParams(
        nrRounds = 100,
        boosterParams = boosterParams)

    val dMatrix = regulatorCSC.copyToUnlabeledDMatrix

    val targetGene = "G666"

    dMatrix.setLabel(ds.filter(_.gene == targetGene).first.response)

    val booster0 = createBooster(boosterParams, dMatrix)

    (0 until 20).foreach(round => booster0.update(dMatrix, round))

    val dump20   = booster0.getModelDump(null, true)
    val result20 = InferXGBoostRegulations.toRawRegulations(targetGene, regulators, booster0, params)

    val booster0Bytes = booster0.toByteArray

    val booster1 = loadBooster(booster0Bytes, boosterParams)

    (20 until 40).foreach(round => booster1.update(dMatrix, round))

    val dump40   = booster1.getModelDump(null, true)
    val result40 = InferXGBoostRegulations.toRawRegulations(targetGene, regulators, booster1, params)

    println(result20.mkString("\n"))
    println(result40.mkString("\n"))
  }

}