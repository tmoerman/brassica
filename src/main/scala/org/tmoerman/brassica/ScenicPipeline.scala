package org.tmoerman.brassica

import breeze.linalg.CSCMatrix
import ml.dmlc.xgboost4j.java.{Booster, JXGBoostAccess}
import ml.dmlc.xgboost4j.java.JXGBoostAccess.{createBooster, saveRabbitCheckpoint}
import ml.dmlc.xgboost4j.scala.XGBoostAccess.inner
import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost, XGBoostAccess}
import org.apache.spark.ml.linalg.Vectors.dense
import org.apache.spark.sql.{Dataset, Encoder}
import org.tmoerman.brassica.tuning.CV.makeCVSets
import org.tmoerman.brassica.util.BreezeUtils._

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
  def inferRegulations(expressionsByGene: Dataset[ExpressionByGene],
                       candidateRegulators: Set[Gene],
                       targets: Set[Gene] = Set.empty,
                       params: XGBoostRegressionParams = XGBoostRegressionParams(),
                       nrPartitions: Option[Int] = None): Dataset[Regulation] = {

    import expressionsByGene.sparkSession.implicits._

    val f = ComputeXGBoostRegulations(params)(_, _, _, _)

    computePartitioned(expressionsByGene, candidateRegulators, targets, nrPartitions)(f)
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
                          nrPartitions: Option[Int] = None): Dataset[OptimizedHyperParams] = {

    import expressionsByGene.sparkSession.implicits._

    val f = ComputeXGBoostOptimizedHyperParams(params)(_, _, _, _)

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
  private[brassica] def computePartitioned[T : Encoder : ClassTag](
    expressionsByGene: Dataset[ExpressionByGene],
    candidateRegulators: Set[Gene],
    targets: Set[Gene],
    nrPartitions: Option[Int])
   (partitionTaskFactory: (List[Gene], CSCMatrix[Expression], Int, Iterator[ExpressionByGene]) => PartitionTask[T]): Dataset[T] = {

    val spark = expressionsByGene.sparkSession
    val sc = spark.sparkContext

    import spark.implicits._

    val regulators   = expressionsByGene.genes.filter(candidateRegulators.contains)
    val regulatorCSC = reduceToRegulatorCSCMatrix(expressionsByGene, regulators)

    val regulatorsBroadcast   = sc.broadcast(regulators)
    val regulatorCSCBroadcast = sc.broadcast(regulatorCSC)

    def isTarget(e: ExpressionByGene) = containedIn(targets)(e.gene)

    nrPartitions
      .map(expressionsByGene.repartition) // FIXME always cache after repartition (cfr. Heather Miller Coursera)
      .getOrElse(expressionsByGene)
      .filter(isTarget _)
      .rdd
      .mapPartitionsWithIndex{ case (partitionIndex, partitionIterator) => {

        if (partitionIterator.nonEmpty) {
          println(s"partition $partitionIndex")

          val regulators    = regulatorsBroadcast.value
          val regulatorCSC  = regulatorCSCBroadcast.value
          val partitionTask = partitionTaskFactory.apply(regulators, regulatorCSC, partitionIndex, partitionIterator)

          partitionIterator.flatMap(expressionByGene => {
            val results = partitionTask(expressionByGene)

            if (partitionIterator.isEmpty) {
              partitionTask.dispose()
            }

            results
          })
        } else {
          val empty: Iterator[T] = Nil.iterator
          empty
        }

      }}
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

}

/**
  * Exposes the two API methods relevant to the computePartitioned function.
  *
  * @tparam T Generic result type.
  */
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
                                     regulatorCSC: CSCMatrix[Expression],
                                     partitionIndex: Int,
                                     partitionIterator: Iterator[ExpressionByGene]) extends PartitionTask[Regulation] {
  import params._

  private[this] val cachedRegulatorDMatrix = toDMatrix(regulatorCSC)

  override def dispose(): Unit = {
    cachedRegulatorDMatrix.delete()
  }

  /**
    * @return Returns the inferred Regulation instances for one ExpressionByGene instance.
    */
  override def apply(expressionByGene: ExpressionByGene): Iterable[Regulation] = {
    val targetGene = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    println(s"-> target: $targetGene, regulator: $targetIsRegulator, partition: $partitionIndex")

    // remove the target gene column if target gene is a regulator
    val cleanedDMatrixGenesToIndices =
      regulators
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene } // remove the target from the predictors

    // slice the target gene column from the regulator CSC matrix and create a new DMatrix

    val (targetDMatrix, disposeFn) =
      if (targetIsRegulator) {
        val cleanRegulatorCSC = regulatorCSC(0 until regulatorCSC.rows, cleanedDMatrixGenesToIndices.map(_._2))
        val cleanRegulatorDMatrix = toDMatrix(cleanRegulatorCSC)

        (cleanRegulatorDMatrix, () => cleanRegulatorDMatrix.delete())
      } else {
        (cachedRegulatorDMatrix, () => Unit)
      }
    targetDMatrix.setLabel(expressionByGene.response)

    // train the model
    val booster = XGBoost.train(targetDMatrix, boosterParams, nrRounds)

    val cleanedDMatrixGenes = cleanedDMatrixGenesToIndices.map(_._1)
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
                                              regulatorCSC: CSCMatrix[Expression],
                                              partitionIndex: Int,
                                              partitionIterator: Iterator[ExpressionByGene]) extends PartitionTask[OptimizedHyperParams] {
  import params._

  private[this] val cvSets = makeCVSets(nrFolds, regulatorCSC.rows, seed + partitionIndex)

  private[this] val cachedRegulatorDMatrix = toDMatrix(regulatorCSC)

  private[this] val cachedNFoldDMatrices = toNFoldMatrices(cachedRegulatorDMatrix, cvSets)

  private[this] val rng = random(seed + partitionIndex)

  override def dispose(): Unit = {
    cachedRegulatorDMatrix.delete()

    dispose(cachedNFoldDMatrices)
  }

  /**
    * @return Returns the optimized hyperparameters for one ExpressionByGene instance.
    */
  override def apply(expressionByGene: ExpressionByGene): Iterable[OptimizedHyperParams] = {
    val targetGene = expressionByGene.gene
    val targetIsRegulator = regulators.contains(targetGene)

    println(s"-> target: $targetGene, regulator: $targetIsRegulator, partition: $partitionIndex")

    // remove the target gene column if target gene is a regulator
    val cleanedDMatrixGenesToIndices =
      regulators
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene } // remove the target from the predictors

    // slice the target gene column from the regulator CSC matrix and create a new DMatrix
    val (targetDMatrix, disposeFn) =
      if (targetIsRegulator) {
        val cleanRegulatorCSC = regulatorCSC(0 until regulatorCSC.rows, cleanedDMatrixGenesToIndices.map(_._2))
        val cleanRegulatorDMatrix = toDMatrix(cleanRegulatorCSC)

        (cleanRegulatorDMatrix, () => cleanRegulatorDMatrix.delete())
      } else {
        (cachedRegulatorDMatrix, () => Unit)
      }
    targetDMatrix.setLabel(expressionByGene.response)

    // create n-fold matrices
    val (targetNFoldDMatrices, disposeNFold) =
      if (targetIsRegulator) {
        val cleanNFoldDMatrices = toNFoldMatrices(targetDMatrix, cvSets)

        (cleanNFoldDMatrices, () => {
          disposeFn(); dispose(cleanNFoldDMatrices)
        })
      } else {
        (cachedNFoldDMatrices, () => Unit)
      }

    // optimize the params for the current n-fold CV sets

    val trials =
      (1 to nrTrials)
        .map(trial => {
          val sampledParams =
            boosterParamSpace.map{ case (key, generator) => (key, generator(rng)) }

          // println(s"$targetGene, trial $trial")

          val loss =
            computeLoss(
              targetNFoldDMatrices,
              sampledParams
                + ("eval_metric" -> evalMetric)
                + ("silent" -> 1)
                + ("nthread" -> 1)
            )

          // println(s"target: $targetGene \t trial: $trial \t loss: $loss \t $sampledParams")

          (sampledParams, loss)
        })

    def toOptimizedHyperParams(sampledParams: BoosterParams, loss: Float) = {
      val sorted   = sampledParams.toSeq.sortBy(_._1)
      val keys     = sorted.map(_._1).mkString(",")
      val values   = sorted.map(_._2.toString.toDouble).toArray

      OptimizedHyperParams(
        target = targetGene,
        metric = evalMetric,
        nrBoostingRounds = nrRounds,
        loss = loss,
        keys = keys,
        values = dense(values))
    }

    disposeNFold.apply()

    if (onlyBest) {
      val (sampledParams, loss) = trials.minBy(_._2)

      Iterable(toOptimizedHyperParams(sampledParams, loss))
    } else {
      trials.map{ case (sampledParams, loss) => toOptimizedHyperParams(sampledParams, loss)}
    }
  }

  val names = Array("train", "test")

  def computeLoss(nFoldDMatrices: List[(DMatrix, DMatrix)], sampledParameters: BoosterParams): Float = {
    val cvPacks: List[(DMatrix, DMatrix, Booster)] =
      nFoldDMatrices
        .map{ case (train, test) => (train, test, createBooster(sampledParameters, train, test)) }

    // TODO early stopping strategy or elbow calculation.
    val foldResultsPerRound =
      (0 until nrRounds)
        .foreach(round => {
//          val foldResults =
            cvPacks
              .foreach{ case (train, test, booster) =>
                val train4j = inner(train)
                //val test4j  = inner(test)
                //val mats4j  = Array(train4j, test4j)

                booster.update(train4j, round)
                saveRabbitCheckpoint(booster)
                // if (round == nrRounds -1) booster.evalSet(mats4j, names, round) else "noppes"
              }
              //.toArray
        })


    val lastFoldResults = cvPacks.map{ case (train, test, booster) =>
      val train4j = inner(train)
      val test4j  = inner(test)
      val mats4j  = Array(train4j, test4j)

      val result = booster.evalSet(mats4j, names, nrRounds - 1)

      saveRabbitCheckpoint(booster)

      result
    }

    // TODO also return round nr ~ last for now

    // val (lastRound, lastFoldResults) = foldResultsPerRound.last

    val (lastTrainingLoss, lastTestLoss) = toEvalScores(lastFoldResults)

    cvPacks.map(_._3).foreach(_.dispose())

    lastTestLoss
  }

  private[this] def toEvalScores(foldResults: Seq[String]): (Float, Float) = {
    val averageEvalScores =
      foldResults
        .flatMap(foldResult => {
          foldResult
            .split("\t")
            .drop(1) // drop the index
            .map(_.split(":") match {
              case Array(key, value) => (key, value.toFloat)
            })})
        .groupBy(_._1)
        .mapValues(x => x.map(_._2).sum / x.length)

    (averageEvalScores("train-rmse"), averageEvalScores("test-rmse"))
  }

  private[this] def toNFoldMatrices(matrix: DMatrix, cvSets: List[(Array[CellIndex], Array[CellIndex])]) =
    cvSets.map{ case (trainIndices, testIndices) => (matrix.slice(trainIndices), matrix.slice(testIndices)) }

  private[this] def dispose(matrices: List[(DMatrix, DMatrix)]): Unit =
    matrices.foreach{ case (a, b) => {
      a.delete()
      b.delete()
    }}

}