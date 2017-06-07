package org.aertslab.grnboost

import java.io.File

import breeze.linalg.CSCMatrix
import org.aertslab.grnboost.algo.{ComputeCVLoss, InferRegulationsIterated, InferXGBoostRegulations, OptimizeXGBoostHyperParams}
import org.aertslab.grnboost.cases.DataReader._
import org.aertslab.grnboost.util.IOUtils._
import org.aertslab.grnboost.util.TimeUtils._
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.reflect.ClassTag

/**
  * @author Thomas Moerman
  */
object GRNBoost {

  /**
    * Main application entry point.
    * @param args
    */
  def main(args: Array[String]): Unit =
    CLI(args: _*) match {
      case Some(Config(Some(inferenceConfig), None)) => infer(inferenceConfig)
      case _                                         => Unit
    }

  /**
    * Perform GRN inference in function of specified InferenceConfig.
    * @param inferenceConfig
    */
  def infer(inferenceConfig: InferenceConfig): Unit = {
    import inferenceConfig._

    val spark =
      SparkSession
        .builder
        .appName(GRN_BOOST)
        .getOrCreate

    import spark.implicits._

    val (_, wallTime) = profile {

      val params =
        XGBoostRegressionParams(
          nrRounds = nrBoostingRounds.getOrElse(1000), // FIXME early stop mechanism needed!
          boosterParams = boosterParams)

      val ds  = readExpressionsByGene(spark, input.get.getAbsolutePath, nrHeaders = inputHeaders)
      val TFs = readRegulators(regulators.get.getAbsolutePath)

      val (sampleIndices, maybeSampled) =
        ds.subSample(sampleSize)

      val regulations =
        if (iterated)
          inferRegulationsIterated(maybeSampled, TFs.toSet, targets.toSet, params, nrPartitions)
        else
          inferRegulations(maybeSampled, TFs.toSet, targets.toSet, params, nrPartitions)

      val maybeTruncated =
        truncate
          .map(nr => regulations.sort($"gain").limit(nr))
          .getOrElse(regulations)

      val sorted =
        maybeTruncated
          .sort($"regulator", $"target", $"gain")

      sorted
        .saveTxt(output.get.getAbsolutePath, delimiter)

      sampleIndices
        .foreach(ids => {
          val sampleLogFile = new File(output.get, "_sample.log")
          writeToFile(sampleLogFile, ids.mkString("\n"))
        })
    }

    val runLogFile = new File(output.get, "_run.log")
    val runLogText =
      s"""
        |* Inference configuration:
        |${inferenceConfig.toString}
        |
        |* Wall time: ${pretty(wallTime)}"}
      """.stripMargin

    writeToFile(runLogFile.getAbsolutePath, runLogText)
  }

  /**
    * @param expressionsByGene
    * @param candidateRegulators
    * @param targets
    * @param params
    * @param nrPartitions
    * @return Returns a Dataset of Regulations.
    */
  def inferRegulationsIterated(expressionsByGene: Dataset[ExpressionByGene],
                               candidateRegulators: Set[Gene],
                               targets: Set[Gene] = Set.empty,
                               params: XGBoostRegressionParams = XGBoostRegressionParams(),
                               nrPartitions: Option[Int] = None): Dataset[Regulation] = {

    val spark = expressionsByGene.sparkSession
    val sc = spark.sparkContext

    import spark.implicits._

    val regulators = expressionsByGene.genes.filter(candidateRegulators.contains)

    assert(regulators.nonEmpty,
      s"no regulators w.r.t. specified candidate regulators ${candidateRegulators.take(3).mkString(",")}...")

    val regulatorCSC = reduceToRegulatorCSCMatrix(expressionsByGene, regulators)

    val regulatorsBroadcast   = sc.broadcast(regulators)
    val regulatorCSCBroadcast = sc.broadcast(regulatorCSC)

    def isTarget(e: ExpressionByGene) = containedIn(targets)(e.gene)

    val onlyTargets =
      expressionsByGene
        .filter(isTarget _)
        .rdd

    nrPartitions
      .map(onlyTargets.repartition(_).cache)
      .getOrElse(onlyTargets)
      .flatMap(
        InferRegulationsIterated(
          params,
          regulatorsBroadcast.value,
          regulatorCSCBroadcast.value)(_))
      .toDS
  }

  /**
    * @param expressionsByGene A Dataset of ExpressionByGene instances.
    * @param candidateRegulators The Set of candidate regulators (TF).
    *                            The term "candidate" is used to imply that not all these regulators are expected
    *                            to be present in the specified List of all genes.
    * @param targets A Set of target genes for which we wish to infer the important regulators.
    *                If empty Set is specified, this is interpreted as: target genes = all genes.
    * @param params The XGBoost regression parameters.
    * @param nrPartitions Optional technical parameter for defining the nr. of Spark partitions to use.
    *
    * @return Returns a Dataset of Regulation instances.
    */
  def inferRegulations(expressionsByGene: Dataset[ExpressionByGene],
                       candidateRegulators: Set[Gene],
                       targets: Set[Gene] = Set.empty,
                       params: XGBoostRegressionParams = XGBoostRegressionParams(),
                       nrPartitions: Option[Int] = None): Dataset[Regulation] = {

    import expressionsByGene.sparkSession.implicits._

    val partitionTaskFactory = InferXGBoostRegulations(params)(_, _, _)

    computePartitioned(expressionsByGene, candidateRegulators, targets, nrPartitions)(partitionTaskFactory)
  }

  /**
    * @param expressionsByGene
    * @param candidateRegulators
    * @param targets
    * @param params
    * @param nrPartitions
    *
    * @return Returns a Dataset of LossByRound instances.
    */
  def computeCVLoss(expressionsByGene: Dataset[ExpressionByGene],
                    candidateRegulators: Set[Gene],
                    targets: Set[Gene] = Set.empty,
                    params: XGBoostRegressionParams = XGBoostRegressionParams(),
                    nrPartitions: Option[Int] = None): Dataset[LossByRound] = {

    import expressionsByGene.sparkSession.implicits._

    val partitionTaskFactory = ComputeCVLoss(params)(_, _, _)

    computePartitioned(expressionsByGene, candidateRegulators, targets, nrPartitions)(partitionTaskFactory)
  }

  /**
    * @param expressionsByGene A Dataset of ExpressionByGene instances.
    * @param candidateRegulators The Set of candidate regulators (TF).
    *                            The term "candidate" is used to imply that not all these regulators are expected
    *                            to be present in the specified List of all genes.
    * @param targets A Set of target genes for which we wish to infer the important regulators.
    *                If empty Set is specified, this is interpreted as: target genes = all genes.
    * @param params The XGBoost hyperparameter optimization parameters.
    * @param nrPartitions Optional technical parameter for defining the nr. of Spark partitions to use.
    *
    * @return Returns a Dataset of OptimizedHyperParams.
    */
  def optimizeHyperParams(expressionsByGene: Dataset[ExpressionByGene],
                          candidateRegulators: Set[Gene],
                          targets: Set[Gene] = Set.empty,
                          params: XGBoostOptimizationParams = XGBoostOptimizationParams(),
                          nrPartitions: Option[Int] = None): Dataset[HyperParamsLoss] = {

    import expressionsByGene.sparkSession.implicits._

    val partitionTaskFactory = OptimizeXGBoostHyperParams(params)(_, _, _)

    computePartitioned(expressionsByGene, candidateRegulators, targets, nrPartitions)(partitionTaskFactory)
  }

  def computePartitioned[T : Encoder : ClassTag](expressionsByGene: Dataset[ExpressionByGene],
                                                 candidateRegulators: Set[Gene],
                                                 targets: Set[Gene],
                                                 nrPartitions: Option[Int])
                                                (partitionTaskFactory: (List[Gene], CSCMatrix[Expression], Int) => PartitionTask[T]): Dataset[T] = {

    val spark = expressionsByGene.sparkSession
    val sc = spark.sparkContext

    import spark.implicits._

    val regulators = expressionsByGene.genes.filter(candidateRegulators.contains)

    assert(regulators.nonEmpty,
      s"no regulators w.r.t. specified candidate regulators ${candidateRegulators.take(3).mkString(",")}...")

    val regulatorCSC = reduceToRegulatorCSCMatrix(expressionsByGene, regulators)

    val regulatorsBroadcast   = sc.broadcast(regulators)
    val regulatorCSCBroadcast = sc.broadcast(regulatorCSC)

    def isTarget(e: ExpressionByGene) = containedIn(targets)(e.gene)

    val onlyTargets =
      if (targets.isEmpty)
        expressionsByGene.rdd
      else
        expressionsByGene.filter(isTarget _).rdd

    val repartitioned =
      nrPartitions
        .map(onlyTargets.repartition(_).cache)
        .getOrElse(onlyTargets)

    repartitioned
      .mapPartitionsWithIndex{ case (partitionIndex, partitionIterator) => {
        if (partitionIterator.nonEmpty) {
          val regulators    = regulatorsBroadcast.value
          val regulatorCSC  = regulatorCSCBroadcast.value
          val partitionTask = partitionTaskFactory.apply(regulators, regulatorCSC, partitionIndex)

          partitionIterator
            .flatMap{ expressionByGene => {
              val results = partitionTask(expressionByGene)

              if (partitionIterator.isEmpty) {
                partitionTask.dispose()
              }

              results
            }}
        } else {
          Nil.iterator.asInstanceOf[Iterator[T]]
        }
      }}
      .toDS
  }

  private[grnboost] def containedIn(targets: Set[Gene]): Gene => Boolean =
    if (targets.isEmpty)
      _ => true
    else
      targets.contains

  /**
    * @param expressionByGene The Dataset of ExpressionByGene instances.
    * @param regulators The ordered List of regulators.
    *
    * @return Returns a CSCMatrix of regulator gene expression values.
    */
  def reduceToRegulatorCSCMatrix(expressionByGene: Dataset[ExpressionByGene],
                                 regulators: List[Gene]): CSCMatrix[Expression] = {

    val nrGenes = regulators.size
    val nrCells = expressionByGene.first.values.size

    val regulatorIndexMap       = regulators.zipWithIndex.toMap
    def isPredictor(gene: Gene) = regulatorIndexMap.contains(gene)
    def cscIndex(gene: Gene)    = regulatorIndexMap.apply(gene)

    expressionByGene
      .rdd
      .filter(e => isPredictor(e.gene))
      .mapPartitions{ it =>
        val matrixBuilder = new CSCMatrix.Builder[Expression](rows = nrCells, cols = nrGenes)

        it.foreach { case ExpressionByGene(gene, expression) =>

          val geneIdx = cscIndex(gene)

          expression
            .foreachActive{ (cellIdx, value) =>
              matrixBuilder.add(cellIdx, geneIdx, value.toFloat)
            }
        }

        Iterator(matrixBuilder.result)
      }
      .treeReduce(_ + _) // https://issues.apache.org/jira/browse/SPARK-2174
  }

}

/**
  * Exposes the two API methods relevant to the computePartitioned function.
  *
  * @tparam T Generic result type.
  */
trait PartitionTask[T] {

  /**
    * @return Returns a resulting iterable of Dataset entries.
    */
  def apply(expressionByGene: ExpressionByGene): Iterable[T]

  /**
    * Dispose used resources.
    */
  def dispose(): Unit

}