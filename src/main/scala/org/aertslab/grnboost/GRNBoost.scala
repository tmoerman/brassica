package org.aertslab.grnboost

import java.lang.Math.min

import breeze.linalg.CSCMatrix
import org.aertslab.grnboost.DataReader._
import org.aertslab.grnboost.algo._
import org.aertslab.grnboost.util.TimeUtils._
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}
import org.apache.spark.util.SizeEstimator
import org.joda.time.DateTime
import org.joda.time.DateTime.now
import org.joda.time.format.DateTimeFormat

import scala.Console.{err, out}
import scala.reflect.ClassTag
import scala.util.{Random, Try}

/**
  * The top-level GRNBoost functions.
  *
  * @author Thomas Moerman
  */
object GRNBoost {

  val ABOUT =
    s"""
      |$GRN_BOOST
      |--------
      |
      |$URL
    """.stripMargin

  /**
    * Main application entry point.
    *
    * @param args The driver program's arguments, an Array of Strings interpreted by the CLI (command line interface)
    *             function, which transforms the args into an Option of Config. If a valid configuration is produced,
    *             a GRNBoost run is performed. Otherwise, feedback is printed to the Java console for user inspection.
    */
  def main(args: Array[String]): Unit =
    CLI(args: _*) match {
      case Some(Config(Some(xgbConfig))) => run(xgbConfig)
      case Some(Config(None))            => out.print(ABOUT)
      case _                             => err.print("Input validation failure occurred, see error message above.")
    }

  /**
    * Inspects the config and dispatches to the appropriate function for the GRNBoost RunMode.
    * Note: although this function has a return type, it possibly performs side effects.
    *
    * @param xgbConfig The configuration parsed from the command line arguments.
    *
    * @return Returns a tuple of possibly updated config and parameter value objects.
    *         This return value is inspected by test routines, but ignored in the main GRNBoost function.
    */
  def run(xgbConfig: XGBoostConfig): (XGBoostConfig, XGBoostRegressionParams) = {
    import xgbConfig._

    val spark =
      SparkSession
        .builder
        .appName(GRN_BOOST)
        .getOrCreate

    val protoParams =
      XGBoostRegressionParams(
        nrFolds = nrFolds,
        boosterParams = boosterParams)

    runMode match {
      case DRY_RUN => (xgbConfig, protoParams)
      case CFG_RUN => run(spark, xgbConfig, protoParams, doInference = false)
      case INF_RUN => run(spark, xgbConfig, protoParams, doInference = true)
    }
  }

  /**
    * The main GRNBoost execution procedure.
    *
    * @param spark The Spark Session
    * @param xgbConfig The configuration parsed from the command line arguments.
    * @param protoParams A set of initial XGBoost regression parameters.
    * @param doInference Flag that specifies whether to perform the GRN inference if true, else only the configuration
    *                    estimation logic.
    *
    * @return Returns a tuple of possibly updated config and parameter value objects.
    *         This return value is inspected by test routines, but ignored in the main GRNBoost function.
    */
  def run(spark: SparkSession,
          xgbConfig: XGBoostConfig,
          protoParams: XGBoostRegressionParams,
          doInference: Boolean) = {

    import xgbConfig._

    val started = now

    val ds = readExpressionsByGene(spark, input.get, skipHeaders, delimiter, missing).cache

    val (sampleCellIndices, maybeSampled) =
      sampleSize
        .map(nrCells => ds.subSample(nrCells))
        .getOrElse(Nil, ds)

    val candidateRegulators = readRegulators(spark, regulators.get)

    val parallelism = nrPartitions.orElse(Some(spark.sparkContext.defaultParallelism))

    // estimation logic
    def estimateNrRounds(ds: Dataset[ExpressionByGene], estimationSet: Either[Int, Set[Gene]]) = {
      val estimationTargets = estimationSet match {
        case Left(estimationTargetSetSize) =>
          new Random(protoParams.seed)
            .shuffle(ds.genes)
            .take(min(estimationTargetSetSize, ds.count).toInt)
            .toSet

        case Right(estimationTargetSet) =>
          estimationTargetSet
      }

      val roundsEstimations = nrBoostingRoundsEstimationsIterated(ds, candidateRegulators, estimationTargets, protoParams, parallelism)

      val estimatedNrRounds = aggregateEstimate(roundsEstimations)

      (estimatedNrRounds, estimationTargets)
    }

    val (finalNrRounds, estimationTargets): (Option[Int], Set[Gene]) = (nrBoostingRounds, estimationSet) match {
      case (None, estimationSet) => estimateNrRounds(maybeSampled, estimationSet)
      case (nrRounds, _)         => (nrRounds, Set.empty)
    }

    val updatedParams =
      protoParams
        .copy(nrRounds = finalNrRounds)

    // inference logic, performed when needed
    if (doInference) {
      import spark.implicits._

      // narrowly-scoped functions closing over xgbConfig values, stitched together with a monad.

      def infer(ds: Dataset[ExpressionByGene]) =
        if (iterated)
          inferRegulationsIterated(ds, candidateRegulators, targets, updatedParams, parallelism)
        else
          inferRegulations(ds, candidateRegulators, targets, updatedParams, parallelism)

      def regularize(ds: Dataset[Regulation]) =
        if (regularized)
          withRegularizationLabels(ds, updatedParams).filter($"include" === 1)
        else
          withRegularizationLabels(ds, updatedParams)

      def normalize(ds: Dataset[Regulation]) =
        if (normalized)
          normalizedByAggregate(ds)
        else
          ds

      def truncate(ds: Dataset[Regulation]) =
        truncated
          .map(nr => ds.sort($"gain".desc).limit(nr))
          .getOrElse(ds)

      def sort(ds: Dataset[Regulation]) = ds.sort($"gain".desc)

      def write(ds: Dataset[Regulation]) = ds.saveTxt(output.get, includeFlags, delimiter)

      // monadic pipeline pattern

      Some(maybeSampled)
        .map(infer)
        .map(regularize)
        .map(normalize)
        .map(truncate)
        .map(sort)
        .foreach(write)
    }

    val updatedXgbConfig =
      xgbConfig
        .copy(estimationSet    = Right(estimationTargets))
        .copy(nrBoostingRounds = finalNrRounds)
        .copy(nrPartitions     = parallelism)

    if (report) {
      writeReports(spark, output.get, makeReport(started, updatedXgbConfig), sampleCellIndices)
    }

    (updatedXgbConfig, updatedParams)
  }

  /**
    * @return Returns a multi-line String containing a human readable report of the inference run.
    */
  def makeReport(started: DateTime, inferenceConfig: XGBoostConfig): String = {
    val finished = now
    val format = DateTimeFormat.forPattern("yyyy-MM-dd:hh.mm.ss")
    val startedPretty  = format.print(started)
    val finishedPretty = format.print(finished)

    s"""
      |# $GRNBoost run log
      |
      |* Started: $startedPretty, finished: $finishedPretty, diff: ${pretty(diff(started, finished))}
      |
      |* Inference configuration:
      |${inferenceConfig.toString}
    """.stripMargin
  }

  /**
    * Write the specified report to the stdout console and possibly to file.
    * Writes the Seq of cell indices (sub-sample) if not empty to file.
    *
    * @param spark The Spark Session
    * @param output The output path for the inference result, folder name is used with a suffix for the file report.
    * @param report The human readable report String.
    * @param cellIndices The cell indices, if a sub-sample was specified. If empty, we assume no sampling was specified
    *                    and all cells are taken into account.
    * @param reportToFile Boolean indicator that specifies whether reports should be written to file.
    */
  def writeReports(spark: SparkSession,
                   output: Path,
                   report: String,
                   cellIndices: Seq[CellIndex],
                   reportToFile: Boolean = false): Unit = {

    out.println(report)

    if (reportToFile) {
      spark
        .sparkContext
        .parallelize(report.split("\n"))
        .coalesce(1)
        .saveAsTextFile(reportOutput(output))

      if (cellIndices.nonEmpty)
        spark
          .sparkContext
          .parallelize(cellIndices)
          .coalesce(1)
          .saveAsTextFile(sampleOutput(output))
    }
  }

  private def reportOutput(output: Path) = s"$output.report.log"
  private def sampleOutput(output: Path) = s"$output.sample.log"

  @Experimental
  def inferRegulationsIterated(expressionsByGene: Dataset[ExpressionByGene],
                               candidateRegulators: Set[Gene],
                               targetGenes: Set[Gene] = Set.empty,
                               params: XGBoostRegressionParams,
                               nrPartitions: Option[Count] = None): Dataset[Regulation] = {

    import expressionsByGene.sparkSession.implicits._

    val taskFactory = InferRegulationsIterated(params)(_, _)

    computeMapped(expressionsByGene, candidateRegulators, targetGenes, nrPartitions)(taskFactory)
  }

  def inferRegulations(expressionsByGene: Dataset[ExpressionByGene],
                       candidateRegulators: Set[Gene],
                       targetGenes: Set[Gene] = Set.empty,
                       params: XGBoostRegressionParams,
                       nrPartitions: Option[Count] = None): Dataset[Regulation] = {

    import expressionsByGene.sparkSession.implicits._

    val partitionTaskFactory = InferRegulations(params)(_, _, _)

    computePartitioned(expressionsByGene, candidateRegulators, targetGenes, nrPartitions)(partitionTaskFactory)
  }


  def nrBoostingRoundsEstimationsIterated(expressionsByGene: Dataset[ExpressionByGene],
                                          candidateRegulators: Set[Gene],
                                          targetGenes: Set[Gene] = Set.empty,
                                          params: XGBoostRegressionParams,
                                          nrPartitions: Option[Count] = None): Dataset[RoundsEstimation] = {

    import expressionsByGene.sparkSession.implicits._

    val taskFactory = EstimateNrBoostingRoundsIterated(params)(_, _)

    computeMapped(expressionsByGene, candidateRegulators, targetGenes, nrPartitions)(taskFactory)
  }

  @deprecated
  def nrBoostingRoundsEstimations(expressionsByGene: Dataset[ExpressionByGene],
                                  candidateRegulators: Set[Gene],
                                  targetGenes: Set[Gene] = Set.empty,
                                  params: XGBoostRegressionParams,
                                  nrPartitions: Option[Count] = None): Dataset[RoundsEstimation] = {

    import expressionsByGene.sparkSession.implicits._

    val partitionTaskFactory = EstimateNrBoostingRounds(params)(_, _, _)

    computePartitioned(expressionsByGene, candidateRegulators, targetGenes, nrPartitions)(partitionTaskFactory)
  }

  /**
    * @param estimations The Dataset of RoundsEstimation instances
    * @param agg The aggregation function. Default = max.
    * @return Returns the final estimate of the nr of boosting rounds as a Try Option.
    */
  def aggregateEstimate(estimations: Dataset[RoundsEstimation],
                        agg: Column => Column = max) = {

    import estimations.sparkSession.implicits._

    Try {
      estimations
        .select(agg($"rounds"))
        .first
        .getInt(0)
    }.toOption
  }

  /**
    * Alternative template function that works with batch-iterated XGBoost matrices instead of copied anc cached ones.
    * A factory function creates the task executed in each flatMap step.
    *
    * @return Returns a Dataset of generic type equal to the generic type of the mapTaskFactory.
    */
  @Experimental
  def computeMapped[T : Encoder : ClassTag](expressionsByGene: Dataset[ExpressionByGene],
                                            candidateRegulators: Set[Gene],
                                            targetGenes: Set[Gene] = Set.empty,
                                            nrPartitions: Option[Count] = None)
                                           (mapTaskFactory: (List[Gene], CSCMatrix[Expression]) => Task[T]): Dataset[T] = {

    val spark = expressionsByGene.sparkSession
    val sc = spark.sparkContext
    import spark.implicits._

    val regulators = expressionsByGene.genes.filter(candidateRegulators.contains)
    assert(regulators.nonEmpty, s"no regulators w.r.t. specified candidate regulators ${candidateRegulators.take(3).mkString(",")}...")

    val regulatorCSC = reduceToRegulatorCSCMatrix(expressionsByGene, regulators)

    val regulatorsBroadcast   = sc.broadcast(regulators)
    val regulatorCSCBroadcast = sc.broadcast(regulatorCSC)

    def targetsOnly(ds: Dataset[ExpressionByGene]) =
      if (targetGenes.isEmpty)
        ds
      else
        ds.filter(e => targetGenes contains e.gene)

    def repartition(rdd: RDD[ExpressionByGene]) =
      nrPartitions
        .map(rdd.repartition(_).cache)
        .getOrElse(rdd)

    def mapTask(rdd: RDD[ExpressionByGene]) =
      rdd
        .flatMap(expressionByGene => {
          val task =
            mapTaskFactory(
              regulatorsBroadcast.value,
              regulatorCSCBroadcast.value)

          task.apply(expressionByGene)
        })

    // monadic pipeline pattern
    Some(expressionsByGene)
      .map(targetsOnly)
      .map(_.rdd)
      .map(repartition)
      .map(mapTask)
      .map(_.toDS)
      .get
  }

  /**
    * Template function that breaks op the inference problem in partition-local iterator transformations in order to
    * keep a handle on cached regulation matrices. A factory function creates the task executed in each iterator step.
    *
    * @return Returns a Dataset of generic type equal to the generic type of the partitionTaskFactory.
    */
  def computePartitioned[T : Encoder : ClassTag](expressionsByGene: Dataset[ExpressionByGene],
                                                 candidateRegulators: Set[Gene],
                                                 targetGenes: Set[Gene],
                                                 nrPartitions: Option[Count])
                                                (partitionTaskFactory: (List[Gene], CSCMatrix[Expression], Partition) => PartitionTask[T]): Dataset[T] = {

    val spark = expressionsByGene.sparkSession
    val sc = spark.sparkContext
    import spark.implicits._

    // FIXME extract these so that the regulatorCSC size can be included in the report
    val regulators = expressionsByGene.genes.filter(candidateRegulators.contains)
    assert(regulators.nonEmpty, s"no regulators w.r.t. specified candidate regulators ${candidateRegulators.take(3).mkString(",")}...")

    val regulatorCSC = reduceToRegulatorCSCMatrix(expressionsByGene, regulators)

    val regulatorsBroadcast   = sc.broadcast(regulators)
    val regulatorCSCBroadcast = sc.broadcast(regulatorCSC)

    def targetsOnly(ds: Dataset[ExpressionByGene]) =
      if (targetGenes.isEmpty)
        ds
      else
        ds.filter(e => targetGenes contains e.gene)

    def repartition(rdd: RDD[ExpressionByGene]) =
      nrPartitions
        .map(rdd.repartition(_).cache)
        .getOrElse(rdd)

    def mapPartitionTask(rdd: RDD[ExpressionByGene]) =
      rdd
        .mapPartitionsWithIndex{ case (partitionIndex, partitionIterator) => {
          if (partitionIterator.nonEmpty) {
            val partitionTask =
              partitionTaskFactory(
                regulatorsBroadcast.value,
                regulatorCSCBroadcast.value,
                partitionIndex)

            partitionIterator
              .flatMap{ expressionByGene => {
                val results = partitionTask.apply(expressionByGene)

                if (partitionIterator.isEmpty) {
                  partitionTask.dispose()
                }

                results
              }}
          } else
            Nil.iterator.asInstanceOf[Iterator[T]]
        }}

    // monadic pipeline pattern
    Some(expressionsByGene)
      .map(targetsOnly)
      .map(_.rdd)
      .map(repartition)
      .map(mapPartitionTask)
      .map(_.toDS)
      .get
  }

  /**
    * GRNBoost assumes that the data will contain a substantial amount of zeros, motivating the use of a CSC sparse
    * matrix as the data structure that will be broadcast to the workers.
    *
    * @param expressionsByGene The Dataset of ExpressionByGene instances.
    * @param regulators The ordered List of regulators.
    *
    * @return Returns a CSCMatrix of regulator gene expression values.
    */
  def reduceToRegulatorCSCMatrix(expressionsByGene: Dataset[ExpressionByGene],
                                 regulators: List[Gene]): CSCMatrix[Expression] = {
    val nrGenes = regulators.size
    val nrCells = expressionsByGene.first.values.size

    val regulatorIndexMap       = regulators.zipWithIndex.toMap
    def isPredictor(gene: Gene) = regulatorIndexMap.contains(gene)
    def cscIndex(gene: Gene)    = regulatorIndexMap.apply(gene)

    val regulatorCSC =
      expressionsByGene
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

    println(s"Estimated size of regulator matrix broadcast variable: ${SizeEstimator.estimate(regulatorCSC)} bytes")

    regulatorCSC
  }

}

/**
  * Exposes the API method relevant to the computeMapped function
  *
  * @tparam T Generic result type.
  */
trait Task[T] {

  /**
    * @param expressionByGene The current target gene and its expression vector.
    * @return Returns a resulting iterable of Dataset entries.
    */
  def apply(expressionByGene: ExpressionByGene): Iterable[T]

}

/**
  * Exposes the two API methods relevant to the computePartitioned function.
  *
  * @tparam T Generic result type.
  */
trait PartitionTask[T] extends Task[T] {

  /**
    * Dispose used resources.
    */
  def dispose(): Unit

}