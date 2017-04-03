package org.tmoerman.brassica

import breeze.linalg.{CSCMatrix, SliceMatrix}
import ml.dmlc.xgboost4j.java.DMatrix.SparseType.CSC
import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import org.apache.spark.sql.{Dataset, Encoder}
import org.tmoerman.brassica.tuning.Tuning.makeCVSets
import resource.makeManagedResource

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

    computePartitioned(expressionsByGene, candidateRegulators, targets, nrPartitions)(params, ComputeXGBoostRegulations)
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

    computePartitioned(expressionsByGene, candidateRegulators, targets, nrPartitions)(params, ComputeXGBoostOptimizedHyperParams)
  }

  /**
    * @param params The task parameters.
    * @param task A task producing T instances.
    *
    * @tparam P Generic parameter type.
    * @tparam T Generic result Product (Tuple) type.
    *
    * @return Returns a Dataset of T instances.
    */
  private[brassica] def computePartitioned[P, T : Encoder : ClassTag](expressionsByGene: Dataset[ExpressionByGene],
                                                                      candidateRegulators: Set[Gene],
                                                                      targets: Set[Gene],
                                                                      nrPartitions: Option[Int])
                                                                     (params: P, task: TargetGeneTask[P, T]): Dataset[T] = {
    val spark = expressionsByGene.sparkSession
    val sc = spark.sparkContext

    import spark.implicits._

    val regulators   = expressionsByGene.genes.filter(candidateRegulators.contains)
    val regulatorCSC = toRegulatorCSCMatrix(expressionsByGene, regulators)

    val regulatorsBroadcast   = sc.broadcast(regulators)
    val regulatorCSCBroadcast = sc.broadcast(regulatorCSC)

    def isTarget(e: ExpressionByGene) = containedIn(targets)(e.gene)

    nrPartitions
      .map(expressionsByGene.repartition(_).cache) // always cache after repartition (cfr. Heather Miller Coursera)
      .getOrElse(expressionsByGene)
      .filter(isTarget _)
      .rdd
      .mapPartitions(partitionIterator => {

        val regulators   = regulatorsBroadcast.value
        val regulatorCSC = regulatorCSCBroadcast.value
        val regulatorDMatrix = toDMatrix(regulatorCSC)

        partitionIterator.flatMap(expressionByGene => {
            val result = withManagedTrainingDMatrix(expressionByGene, regulators, regulatorCSC, regulatorDMatrix)(params, task)

            if (partitionIterator.isEmpty) {
              regulatorDMatrix.delete()
            }

            result
          })
      })
      .toDS
  }

  /**
    * @param expressionByGene The Dataset of ExpressionByGene instances.
    * @param regulators The ordered List of regulators.
    *
    * @return Returns a CSCMatrix of regulator gene expression values.
    */
  def toRegulatorCSCMatrix(expressionByGene: Dataset[ExpressionByGene],
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

  /**
    * @return Returns the task result for the specified params, computed with respect to a managed training matrix.
    */
  private[brassica] def withManagedTrainingDMatrix[P, T : Encoder : ClassTag](expressionByGene: ExpressionByGene,
                                                                              regulators: List[Gene],
                                                                              regulatorCSC: CSCMatrix[Expression],
                                                                              regulatorDMatrix: DMatrix)
                                                                             (params: P, task: TargetGeneTask[P, T]): Iterable[T] = {
    val targetGene: Gene = expressionByGene.gene
    val trainingDMatrixGenesToIndices =
      regulators
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene } // remove the target from the predictors
    val trainingDMatrixGenes       = trainingDMatrixGenesToIndices.map(_._1)
    val trainingDMatrixGeneIndices = trainingDMatrixGenesToIndices.map(_._2)

    toManagedTrainingDMatrix(expressionByGene, regulators, regulatorCSC, regulatorDMatrix, trainingDMatrixGeneIndices)
      .map(trainingDMatrix => task(targetGene, trainingDMatrix, trainingDMatrixGenes, params))
      .opt
      .get
  }

  /**
    * @return Returns the training DMatrix from which the column of the target gene is sliced,
    *         in case the target gene is itself a predictor gene.
    */
  private[brassica] def toManagedTrainingDMatrix(expressionByGene: ExpressionByGene,
                                                 regulators: List[Gene],
                                                 regulatorCSC: CSCMatrix[Expression],
                                                 regulatorDMatrix: DMatrix,
                                                 trainingDMatrixGeneIndices: List[GeneIndex]) = {
    val targetGene = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    def create = {
      val trainingMatrix = targetIsRegulator match {
        case true  => toDMatrix(regulatorCSC.apply(0 until regulatorCSC.rows, trainingDMatrixGeneIndices))
        case false => regulatorDMatrix
      }

      trainingMatrix.setLabel(expressionByGene.response)
      trainingMatrix
    }

    def dispose(m: DMatrix) = if (targetIsRegulator) m.delete()

    makeManagedResource(create)(dispose)(Nil)
  }

  // TODO write tests for this !!
  private[brassica] def toDMatrix(m: SliceMatrix[Int, Int, Expression]) =
    new DMatrix(m.activeValuesIterator.toArray, m.rows, m.cols, 0f)

  // TODO write tests for this !!
  private[brassica] def toDMatrix(csc: CSCMatrix[Expression]) =
    new DMatrix(csc.colPtrs.map(_.toLong), csc.rowIndices, csc.data, CSC)

  private[brassica] def containedIn(targets: Set[Gene]): Gene => Boolean =
    if (targets.isEmpty)
      _ => true
    else
      targets.contains

}

trait TargetGeneTask[P, T] {

  def apply(targetGene: Gene,
            trainingDMatrix: DMatrix,
            trainingDMatrixGenes: List[Gene],
            params: P): Iterable[T]

}

object ComputeXGBoostRegulations extends TargetGeneTask[XGBoostRegressionParams, Regulation]
  with Serializable {

  /**
    * @param targetGene
    * @param trainingDMatrix
    * @param trainingDMatrixGenes
    * @param params
    * @return Returns inferred Regulations from training a model with the specified parameter map.
    */
  override def apply(targetGene: Gene,
                     trainingDMatrix: DMatrix,
                     trainingDMatrixGenes: List[Gene],
                     params: XGBoostRegressionParams): Iterable[Regulation] = {

    import params._

    val booster = XGBoost.train(trainingDMatrix, boosterParams, nrRounds)

    val result =
      booster
        .getFeatureScore()
        .map { case (feature, score) =>
          val featureIndex  = feature.substring(1).toInt
          val regulatorGene = trainingDMatrixGenes(featureIndex)
          val importance    = score.toFloat

          Regulation(regulatorGene, targetGene, importance)
        }
        .toSeq
        .sortBy(-_.importance)

    booster.dispose

    result
  }

}

object ComputeXGBoostOptimizedHyperParams extends TargetGeneTask[XGBoostOptimizationParams, OptimizedHyperParams]
  with Serializable {

  // TODO granularity is incorrect: all regression should be performed with the same CV packs

  override def apply(targetGene: Gene,
                     trainingDMatrix: DMatrix,
                     trainingDMatrixGenes: List[Gene],
                     params: XGBoostOptimizationParams): Iterable[OptimizedHyperParams] = {

    import params._

    val cvPack =
      makeCVSets(nrFolds, trainingDMatrix.rowNum.toInt, seed)
        .map{ case (train, test) => (trainingDMatrix.slice(train), trainingDMatrix.slice(test)) }

    // TODO instantiate

    // infer reasonable nr of boosting rounds ~ early stopping.

    ???
  }

}