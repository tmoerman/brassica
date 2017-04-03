package org.tmoerman.brassica

import java.io.Closeable

import breeze.linalg.{CSCMatrix, SliceMatrix}
import ml.dmlc.xgboost4j.java.DMatrix.SparseType.CSC
import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import org.apache.spark.sql.{Dataset, Encoder}
import org.tmoerman.brassica.ScenicPipeline.toDMatrix
import org.tmoerman.brassica.tuning.CV.makeCVSets

import scala.reflect.ClassTag

/**
  * @author Thomas Moerman
  */
object ScenicPipeline {

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
  def computeRegulations(expressionsByGene: Dataset[ExpressionByGene],
                         candidateRegulators: Set[Gene],
                         targets: Set[Gene] = Set.empty,
                         params: XGBoostRegressionParams = XGBoostRegressionParams(),
                         nrPartitions: Option[Int] = None): Dataset[Regulation] = {

    import expressionsByGene.sparkSession.implicits._

    val f = new ComputeXGBoostRegulations(params)(_, _)

    computePartitioned[Regulation](expressionsByGene, candidateRegulators, targets, nrPartitions)(f)
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
  def computeOptimizedHyperParams(expressionsByGene: Dataset[ExpressionByGene],
                                  candidateRegulators: Set[Gene],
                                  targets: Set[Gene] = Set.empty,
                                  params: XGBoostOptimizationParams = XGBoostOptimizationParams(),
                                  nrPartitions: Option[Int] = None): Dataset[OptimizedHyperParams] = {

    import expressionsByGene.sparkSession.implicits._

    val f = new ComputeXGBoostOptimizedHyperParams(params)(_, _)

    computePartitioned(expressionsByGene, candidateRegulators, targets, nrPartitions)(f)
  }

  /**
    *
    *
    * @param expressionsByGene
    * @param candidateRegulators
    * @param targets
    * @param nrPartitions
    * @param partitionTaskFactory
    * @tparam T Generic result Product (Tuple) type.
    * @return Returns a Dataset of T instances.
    */
  private[brassica] def computePartitioned[T : Encoder : ClassTag](expressionsByGene: Dataset[ExpressionByGene],
                                                                   candidateRegulators: Set[Gene],
                                                                   targets: Set[Gene],
                                                                   nrPartitions: Option[Int])
                                                                  (partitionTaskFactory: (List[Gene], CSCMatrix[Expression]) => PartitionTask[T]): Dataset[T] = {
    val spark = expressionsByGene.sparkSession
    val sc = spark.sparkContext

    import spark.implicits._

    val regulators   = expressionsByGene.genes.filter(candidateRegulators.contains)
    val regulatorCSC = reduceToRegulatorCSCMatrix(expressionsByGene, regulators)

    val regulatorsBroadcast   = sc.broadcast(regulators)
    val regulatorCSCBroadcast = sc.broadcast(regulatorCSC)

    def isTarget(e: ExpressionByGene) = containedIn(targets)(e.gene)

    nrPartitions
      .map(expressionsByGene.repartition(_).cache) // always cache after repartition (cfr. Heather Miller Coursera)
      .getOrElse(expressionsByGene)
      .filter(isTarget _)
      .rdd
      .mapPartitions(partitionIterator => {

        val regulators    = regulatorsBroadcast.value
        val regulatorCSC  = regulatorCSCBroadcast.value
        val partitionTask = partitionTaskFactory.apply(regulators, regulatorCSC)

        partitionIterator.flatMap(expressionByGene => {
          val results = partitionTask(expressionByGene)

          if (partitionIterator.isEmpty) {
            partitionTask.dispose()
          }

          results
        })

      })
      .toDS
  }

  private[brassica] def containedIn(targets: Set[Gene]): Gene => Boolean =
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

    val regulatorIndexMap = regulators.zipWithIndex.toMap
    def isPredictor(gene: Gene) = regulatorIndexMap.contains(gene)
    def cscIndex(gene: Gene) = regulatorIndexMap.apply(gene)

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
      .reduce(_ += _)
  }

  // TODO write tests for this !!
  private[brassica] def toDMatrix(m: SliceMatrix[Int, Int, Expression]) =
    new DMatrix(m.activeValuesIterator.toArray, m.rows, m.cols, 0f)

  // TODO write tests for this !!
  private[brassica] def toDMatrix(csc: CSCMatrix[Expression]) =
    new DMatrix(csc.colPtrs.map(_.toLong), csc.rowIndices, csc.data, CSC)

}

abstract class ScopedResource[T](t: T) extends Closeable with Serializable

trait PartitionTask[T] {

  /**
    * @param expressionByGene
    * @return Returns the result for one ExpressionByGene instance.
    */
  def apply(expressionByGene: ExpressionByGene): Iterable[T]

  /**
    * Dispose used resources.
    */
  def dispose(): Unit

}

case class ComputeXGBoostRegulations(params: XGBoostRegressionParams)
                                    (regulators: List[Gene],
                                     regulatorCSC: CSCMatrix[Expression]) extends PartitionTask[Regulation] {
  import params._

  private[this] val cachedRegulatorDMatrix = toDMatrix(regulatorCSC)

  override def dispose() = cachedRegulatorDMatrix.delete()

  /**
    * @param expressionByGene
    * @return Returns the inferred Regulation instances for one ExpressionByGene instance.
    */
  override def apply(expressionByGene: ExpressionByGene): Iterable[Regulation] = {
    val targetGene        = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    // remove the target gene column if target gene is a regulator
    val cleanedDMatrixGenesToIndices =
      regulators
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene } // remove the target from the predictors
    val cleanedDMatrixGenes       = cleanedDMatrixGenesToIndices.map(_._1)
    val cleanedDMatrixGeneIndices = cleanedDMatrixGenesToIndices.map(_._2)

    // slice the target gene column from the regulator CSC matrix and create a new DMatrix
    val (targetDMatrix, disposeFn) = targetIsRegulator match {
      case true  =>
        val cleanRegulatorCSC     = regulatorCSC(0 until regulatorCSC.rows, cleanedDMatrixGeneIndices)
        val cleanRegulatorDMatrix = toDMatrix(cleanRegulatorCSC)

        (cleanRegulatorDMatrix, () => cleanRegulatorDMatrix.delete())
      case false =>
        (cachedRegulatorDMatrix, () => Unit)
    }
    targetDMatrix.setLabel(expressionByGene.response)

    // train the model
    val booster = XGBoost.train(targetDMatrix, boosterParams, nrRounds)

    val result =
      booster
        .getFeatureScore()
        .map { case (feature, score) =>
          val featureIndex  = feature.substring(1).toInt
          val regulatorGene = cleanedDMatrixGenes(featureIndex)
          val importance    = score.toFloat

          Regulation(regulatorGene, targetGene, importance)
        }
        .toSeq
        .sortBy(-_.importance)

    // clean up resources
    booster.dispose
    disposeFn.apply()

    result
  }

}

case class ComputeXGBoostOptimizedHyperParams(params: XGBoostOptimizationParams)
                                             (regulators: List[Gene],
                                              regulatorCSC: CSCMatrix[Expression]) extends PartitionTask[OptimizedHyperParams] {
  import params._

  val cvSets = makeCVSets(nrFolds, regulatorCSC.rows, seed)

  val cachedRegulatorDMatrix = toDMatrix(regulatorCSC)

  val cachedCVnFoldDMatrices =
    cvSets.map{ case (trainIndices, testIndices) =>
      (cachedRegulatorDMatrix.slice(trainIndices), cachedRegulatorDMatrix.slice(testIndices)) }

  override def dispose(): Unit = {
    cachedRegulatorDMatrix.delete()

    cachedCVnFoldDMatrices.foreach{ case (a, b) => {
      a.delete()
      b.delete()
    }}
  }

  /**
    * @param expressionByGene
    * @return Returns the optimized hyperparameters for one ExpressionByGene instance.
    */
  override def apply(expressionByGene: ExpressionByGene): Iterable[OptimizedHyperParams] = {
    val targetGene        = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    ???
  }
  
}