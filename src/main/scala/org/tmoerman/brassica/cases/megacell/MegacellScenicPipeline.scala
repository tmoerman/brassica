package org.tmoerman.brassica.cases.megacell

import breeze.linalg.{CSCMatrix, Matrix, SparseVector}
import org.apache.spark.sql.SparkSession
import org.tmoerman.brassica.ScenicPipeline._
import org.tmoerman.brassica.cases.megacell.MegacellReader._
import ml.dmlc.xgboost4j.scala.DMatrix
import ml.dmlc.xgboost4j.scala.XGBoost
import org.tmoerman.brassica.{ExpressionValue, Gene, XGBoostParams}

import scala.collection.mutable.ListBuffer

/**
  * @author Thomas Moerman
  */
object MegacellScenicPipeline {

  def apply(spark: SparkSession,
            hdf5Path: String,
            parquetPath: String,
            candidateRegulators: List[Gene],
            targets: List[Gene] = Nil,
            xgBoostParams: XGBoostParams = XGBoostParams()): Unit = {

    val sc = spark.sparkContext

    lazy val df = spark.read.parquet(parquetPath) // (gene, columnVector)

    val (nrCells, nrGenes) = readDimensions(hdf5Path).get

    val genes = readGeneNames(hdf5Path).get

    val candidateRegulatorIndexMap = regulatorIndexMap(genes, candidateRegulators)

    val candidateRegulatorIndices = candidateRegulatorIndexMap.values.toSeq

    val (csc, cscIndexMap) = readCSCMatrix(hdf5Path, onlyGeneIndices = Some(candidateRegulatorIndices)).get

    val regulatorCSCIndexLookup = (gene: Gene) =>
      candidateRegulatorIndexMap
        .get(gene)
        .map(cscIndexMap)

    assert(csc.rows == nrCells)
    assert(csc.cols == candidateRegulatorIndices.size)

    val cscBroadcast    = sc.broadcast(csc)
    val lookupBroadcast = sc.broadcast(regulatorCSCIndexLookup)
    val genesBroadcast  = sc.broadcast(genes)

    val isTarget = targets match {
      case Nil => (_: Gene) => true
      case _   => targets.toSet.contains _
    }

    import xgBoostParams._

    df
      .rdd
      .filter(row => {
        val rowGene   = ???
        //TODO filter with isTarget
        true })
      .map(row => {
        val csc = cscBroadcast.value

        val rowGene: Gene = ???
        val rowVector: SparseVector[ExpressionValue] = ???

        // create a CSC with target removed, if it is present among the regulators.
        //TODO decouple index computation and csc slicing

//        val cleanRegulatorIndices =
//          lookupBroadcast
//            .value
//            .apply(rowGene)
//            .map(index =>
//
//            )

        val cleanCSC: Matrix[Int] =
          lookupBroadcast
            .value
            .apply(rowGene)
            .map(indexToRemove => {
              val rowRange = 0 until csc.rows
              val colRange = ListBuffer.range(0, csc.cols).remove(indexToRemove)

              csc(rowRange, colRange)
            })
            .getOrElse(csc)

        // transform the csc to an XGBoost DMatrix
        val dm: DMatrix = ??? // MegacellReader.toXGBoostDMatrix(cleanCSC)

        // set the response variable
        val y: Array[Float] = ??? // rowVector to Array
        dm.setLabel(y)

        val booster =
          XGBoost
            .train(
              dm,
              boosterParams,
              nrRounds)

        booster
          .getFeatureScore()
          .map{ case (featureIndexString, importance) => {

            val featureIndex = featureIndexString.substring(1).toInt

            val candidateRegulatorIndex =

              ???
          }}

        // extract explanation from booster


        ??? })
  }

}