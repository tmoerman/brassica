package org.aertslab.grnboost

import java.io.File
import java.lang.Math.min

import breeze.linalg.CSCMatrix
import org.aertslab.grnboost.DataReader._
import org.aertslab.grnboost.algo._
import org.aertslab.grnboost.util.TimeUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.aertslab.grnboost.util.IOUtils._
import org.joda.time.DateTime
import org.joda.time.DateTime.now
import org.joda.time.format.DateTimeFormat

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.{Random, Try}

/**
  * @author Thomas Moerman
  */
object GRNBoost {

  val ABOUT =
    """
      |GRNBoost
      |--------
      |
      |https://github.com/aertslab/GRNBoost/
    """.stripMargin

  /**
    * Main application entry point.
    * @param args
    */
  def main(args: Array[String]): Unit =
    CLI(args: _*) match {
      case Some(Config(Some(inferenceConfig))) => run(inferenceConfig)
      case Some(Config(None))                  => println(ABOUT)
      case _                                   => ??? // a.k.a. ka-boom
    }

  /**
    * Perform GRN inference in function of specified InferenceConfig.
    * @param inferenceConfig
    */
  def run(inferenceConfig: InferenceConfig): (InferenceConfig, XGBoostRegressionParams) = {
    import inferenceConfig._

    val started = DateTimeFormat.forPattern("yyyy-MM-dd:hh.mm.ss").print(now)

    val spark =
      SparkSession
        .builder
        .appName(GRN_BOOST)
        .getOrCreate

    val protoParams =
      XGBoostRegressionParams(
        nrRounds = -1,
        nrFolds = nrFolds,
        boosterParams = boosterParams)

    import spark.implicits._

    lazy val parallelism = nrPartitions.orElse(Some(spark.sparkContext.defaultParallelism))

    lazy val ds = readExpressionsByGene(spark, input.get.getAbsolutePath, skipHeaders, delimiter).cache

    lazy val TFs = regulators.map(file => readRegulators(file.getAbsolutePath)).getOrElse(ds.genes).toSet

    lazy val (sampleIndices, maybeSampled) =
      sample
        .map(nr => ds.subSample(nr))
        .getOrElse(None, ds)

    lazy val estimationTargets =
      if (estimationSet.isLeft) {
        val nr = min(estimationSet.left.get, maybeSampled.count).toInt

        new Random(protoParams.seed)
          .shuffle(maybeSampled.genes)
          .take(nr)
          .toSet
      } else {
        estimationSet.right.get
      }

    lazy val estimatedNrRounds =
      estimatedNrBoostingRounds(maybeSampled, TFs, estimationTargets, protoParams, parallelism)

    lazy val updatedInferenceConfig =
      inferenceConfig
        .copy(estimationSet    = Right(estimationTargets))
        .copy(nrBoostingRounds = estimatedNrRounds)
        .copy(nrPartitions     = parallelism)

    lazy val updatedParams =
      nrBoostingRounds
        .orElse(estimatedNrRounds)
        .map(estimation => protoParams.copy(nrRounds = estimation))
        .getOrElse(protoParams)

    lazy val regulations = // pattern: monadic pipeline
      Some(maybeSampled)
        .map(expressionsByGene =>
          if (iterated)
            inferRegulationsIterated(expressionsByGene, TFs, targets, updatedParams, parallelism)
          else
            inferRegulations(expressionsByGene, TFs, targets, updatedParams, parallelism))
        .map(result =>
          if (regularize)
            result.withRegularizationLabels(updatedParams).filter($"include" === 1)
          else
            result.withRegularizationLabels(updatedParams))
        .map(result =>
          truncate
            .map(nr => result.sort($"gain").limit(nr))
            .getOrElse(result))
        .map(_.sort($"regulator", $"target", $"gain"))

    def writeReport(wallTime: Duration,
                    sampleIndices: Option[Seq[CellIndex]],
                    inferenceConfig: InferenceConfig): Unit = if (report) {

      val finished = DateTimeFormat.forPattern("yyyy-MM-dd:hh.mm.ss").print(now)

      val estimationLogFile = new File(s"${output.get}.estimation.log")
      val sampleLogFile     = new File(s"${output.get}.sample.log")
      val runLogFile        = new File(s"${output.get}.run.log")

      writeToFile(
        estimationLogFile,
        "# Genes used for boosting rounds estimation\n\n" + estimationTargets.toSeq.sorted.mkString("\n"))

      sampleIndices.foreach(cellIds =>
        writeToFile(
          sampleLogFile,
          "# Cells sampled\n\n" + cellIds.sorted.mkString("\n")))

      val runLogText =
        s"""
          |# GRNboost run log
          |
          |* Started: $started, finished: $finished, wall time: ${pretty(wallTime)}
          |
          |* Inference configuration:
          |${inferenceConfig.toString}
        """.stripMargin

      writeToFile(runLogFile, runLogText)
    }

    goal match {
      case DRY_RUN =>
        (inferenceConfig, protoParams)

      case CFG_RUN =>
        val (_, wallTime) = profile { updatedParams.hashCode }

        writeReport(wallTime, sampleIndices, updatedInferenceConfig)

        (updatedInferenceConfig, updatedParams)

      case INF_RUN =>
        val (_, wallTime) = profile {
          regulations.foreach(_.saveTxt(output.get.getAbsolutePath, ! regularize, delimiter))
        }

        writeReport(wallTime, sampleIndices, updatedInferenceConfig)

        (updatedInferenceConfig, updatedParams)
    }
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
                               params: XGBoostRegressionParams,
                               nrPartitions: Option[Count] = None): Dataset[Regulation] = {

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
                       params: XGBoostRegressionParams,
                       nrPartitions: Option[Count] = None): Dataset[Regulation] = {

    import expressionsByGene.sparkSession.implicits._

    val partitionTaskFactory = InferRegulations(params)(_, _, _)

    computePartitioned(expressionsByGene, candidateRegulators, targets, nrPartitions)(partitionTaskFactory)
  }

  /**
    * @param expressionsByGene
    * @param candidateRegulators
    * @param targets
    * @param params
    * @param nrPartitions
    *
    * @return Returns a Dataset of RoundsEstimation instances.
    */
  def roundsEstimations(expressionsByGene: Dataset[ExpressionByGene],
                        candidateRegulators: Set[Gene],
                        targets: Set[Gene] = Set.empty,
                        params: XGBoostRegressionParams,
                        nrPartitions: Option[Count] = None): Dataset[RoundsEstimation] = {

    import expressionsByGene.sparkSession.implicits._

    val partitionTaskFactory = EstimateNrBoostingRounds(params)(_, _, _)

    computePartitioned(expressionsByGene, candidateRegulators, targets, nrPartitions)(partitionTaskFactory)
  }

  /**
    * @param expressionsByGene
    * @param candidateRegulators
    * @param targets
    * @param params
    * @param nrPartitions
    *
    * @return
    */
  def estimatedNrBoostingRounds(expressionsByGene: Dataset[ExpressionByGene],
                                candidateRegulators: Set[Gene],
                                targets: Set[Gene] = Set.empty,
                                params: XGBoostRegressionParams,
                                nrPartitions: Option[Count] = None): Option[Int] = Try {

    roundsEstimations(expressionsByGene, candidateRegulators, targets, params, nrPartitions)
      .select(max("rounds"))
      .first
      .getInt(0)

  }.toOption

  /**
    * @param expressionsByGene
    * @param candidateRegulators
    * @param targets
    * @param params
    * @param nrPartitions
    *
    * @return Returns a Dataset of LossByRound instances.
    */
  @deprecated def calculateLossByRound(expressionsByGene: Dataset[ExpressionByGene],
                           candidateRegulators: Set[Gene],
                           targets: Set[Gene] = Set.empty,
                           params: XGBoostRegressionParams,
                           nrPartitions: Option[Count] = None): Dataset[LossByRound] = {
    
    import expressionsByGene.sparkSession.implicits._

    val partitionTaskFactory = CalculateLossByRound(params)(_, _, _)

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
  @deprecated def optimizeHyperParams(expressionsByGene: Dataset[ExpressionByGene],
                          candidateRegulators: Set[Gene],
                          targets: Set[Gene] = Set.empty,
                          params: XGBoostOptimizationParams,
                          nrPartitions: Option[Count] = None): Dataset[HyperParamsLoss] = {

    import expressionsByGene.sparkSession.implicits._

    val partitionTaskFactory = OptimizeXGBoostHyperParams(params)(_, _, _)

    computePartitioned(expressionsByGene, candidateRegulators, targets, nrPartitions)(partitionTaskFactory)
  }

  /**
    * Template function that breaks op the inference problem in partition-local iterator transformations in order to
    * keep a handle on cached regulation matrices. A
    *
    * @return
    */
  def computePartitioned[T : Encoder : ClassTag](expressionsByGene: Dataset[ExpressionByGene],
                                                 candidateRegulators: Set[Gene],
                                                 targets: Set[Gene],
                                                 nrPartitions: Option[Count])
                                                (partitionTaskFactory: (List[Gene], CSCMatrix[Expression], Count) => PartitionTask[T]): Dataset[T] = {

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