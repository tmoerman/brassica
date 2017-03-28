package org.tmoerman.brassica

import breeze.linalg.{CSCMatrix, SliceMatrix}
import ml.dmlc.xgboost4j.java.DMatrix.SparseType.CSC
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import org.apache.spark.sql.Dataset
import org.tmoerman.brassica.util.TimeUtils.{pretty, profile}

/**
  * @author Thomas Moerman
  */
object ScenicPipeline {

  /**
    * @param expressionByGene A DataFrame containing the expression values by gene.
    *                         | gene | value |
    * @param candidateRegulators The Set of candidate regulators (TF).
    *                            The term "candidate" is used to imply that not all these regulators are expected
    *                            to be present in the specified List of all genes.
    * @param targets A Set of target genes for which we wish to infer the important regulators.
    *                If empty Set is specified, this is interpreted as: target genes = all genes.
    * @param params The XGBoost regression parameters.
    * @param nrPartitions Optional technical parameter for defining the nr. of Spark partitions to use.
    * @return Returns a DataFrame with schema:
    *         | regulator_name | target_name | importance |
    */
  def apply(expressionByGene: Dataset[ExpressionByGene],
            candidateRegulators: Set[Gene],
            targets: Set[Gene] = Set.empty,
            params: RegressionParams = RegressionParams(),
            nrPartitions: Option[Int] = None): Dataset[Regulation] = {

    val spark = expressionByGene.sparkSession
    import spark.implicits._

    val regulators = expressionByGene.genes.filter(candidateRegulators.contains)

    val (csc, duration) = profile {
      toRegulatorCSCMatrix(expressionByGene, regulators)
    }

    println(s"constructing CSC matrix took ${pretty(duration)}") // TODO logging framework

    val cscBroadcast        = spark.sparkContext.broadcast(csc)
    val regulatorsBroadcast = spark.sparkContext.broadcast(regulators)

    def isTarget(e: ExpressionByGene) = containedIn(targets)(e.gene)

    nrPartitions
      .map(expressionByGene.repartition)
      .getOrElse(expressionByGene)
      .filter(isTarget _)
      .rdd
      .mapPartitions(it => {
        val csc         = cscBroadcast.value
        val regulators  = regulatorsBroadcast.value
        val fullDMatrix = toDMatrix(csc)

        it.flatMap(expressionByGene => {

          val input  = XGboostInput(expressionByGene, regulators, csc, fullDMatrix, params)

          val result = withManagedTrainingDMatrix(input /* <insert task> */) // TODO extract this to a "task" trait.

          if (it.isEmpty) {
            fullDMatrix.delete()
          }

          result
        })
      })
      .toDS
  }

  /**
    * @param expressionByGene The Dataset of ExpressionByGene instances.
    * @param regulators The ordered List of regulators.
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
    * @param expressionByGene The expression of the target gene.
    * @param regulators The List of genes in the columns of the CSCMatrix.
    * @param csc The CSC matrix of gene expression values, only contains regulators.
    * @param fullDMatrix DMatrix built from the CSC Matrix.
    * @param params RegressionParams
    */
  case class XGboostInput(expressionByGene: ExpressionByGene,
                          regulators: List[Gene],
                          csc: CSCMatrix[Expression],
                          fullDMatrix: DMatrix,
                          params: RegressionParams) {

    def targetGene: Gene = expressionByGene.gene

    /**
      * @return Returns wether the target gene is a regulator.
      */
    def targetIsRegulator: Boolean = regulators.contains(targetGene)

  }

  /**
    * @param input Input value object.
    * @return @return Calculate the importance scores for the regulators of the target gene.
    */
  def withManagedTrainingDMatrix(input: XGboostInput): Iterable[Regulation] = {
    import input._

    println(s"-> $targetGene (regulator? $targetIsRegulator)")

    val trainingDMatrixGenesToIndices =
      regulators
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene } // remove the target from the predictors
    val trainingDMatrixGenes       = trainingDMatrixGenesToIndices.map(_._1)
    val trainingDMatrixGeneIndices = trainingDMatrixGenesToIndices.map(_._2)

    resource
      .makeManagedResource(toTrainingDMatrix(input, trainingDMatrixGeneIndices))(m => if (targetIsRegulator) m.delete())(Nil)
      .map(trainingDMatrix => computeRegulations(targetGene, trainingDMatrix, trainingDMatrixGenes, params))
      .opt
      .get
  }

  /**
    * @param input The XGBoostInput
    * @param trainingDMatrixGeneIndices Subset of CSC matrix column indices to slice the CSC matrix,
    *                                   removing the column for the target gene.
    * @return
    */
  private def toTrainingDMatrix(input: XGboostInput, trainingDMatrixGeneIndices: List[GeneIndex]) = {
    import input._

    lazy val withoutTargetDMatrix = toDMatrix(csc.apply(0 until csc.rows, trainingDMatrixGeneIndices))
    val trainingDMatrix = if (targetIsRegulator) withoutTargetDMatrix else fullDMatrix

    trainingDMatrix.setLabel(expressionByGene.response)
    trainingDMatrix
  }

  /**
    * @param trainingDMatrix The training DMatrix.
    * @param targetGene The target gene.
    * @param trainingDMatrixGenes List of genes in the columns of the training DMatrix.
    * @param params The regression parameters.
    * @return Returns an Iterable of gene Regulation instances.
    */
  @deprecated("wrap in a trait")
  def computeRegulations(targetGene: Gene,
                         trainingDMatrix: DMatrix,
                         trainingDMatrixGenes: List[Gene],
                         params: RegressionParams): Iterable[Regulation] = {
    import params._

    val booster = XGBoost.train(trainingDMatrix, boosterParams, nrRounds)

    toRegulations(booster, targetGene, trainingDMatrixGenes, normalizeImportances)
  }

  /**
    * @param trainingDMatrix The training DMatrix.
    * @param targetGene The target gene.
    * @param trainingDMatrixGenes List of genes in the columns of the training DMatrix.
    * @param params The regression parameters.
    */
  def computeCVScores(targetGene: Gene,
                      trainingDMatrix: DMatrix,
                      trainingDMatrixGenes: List[Gene],
                      params: RegressionParams): Unit = {
    import params._

    // TODO booster is disposable... -> managed resource!
    // val booster = XGBoost.train(trainingDMatrix, boosterParams, nrRounds)
    //
    // val regulations = toRegulations(booster, targetGene, trainingDMatrixGenes, normalize)

    val cv = XGBoost.crossValidation(trainingDMatrix, boosterParams, nrRounds, nrFolds)

    val tuples =
      cv
        .map(_.split("\t").drop(1).map(_.split(":")(1).toFloat))
        .zipWithIndex
        .map{ case (Array(train, test), round) => (round, train, test) }

    // TODO suggested nr rounds cutoff computation ~ cfr. early stopping in XGBoost Python.
    // TODO design a CV data

    println(tuples.mkString(",\n"))
  }

  /**
    * @param booster The Booster instance.
    * @param targetGene The target gene.
    * @param trainingDMatrixGenes List of genes in the columns of the training DMatrix.
    * @return Returns a Seq of Regulation instances, ordered by importance DESC.
    */
  def toRegulations(booster: Booster,
                    targetGene: Gene,
                    trainingDMatrixGenes: List[Gene],
                    normalize: Boolean): Iterable[Regulation] = {

    lazy val sum = booster.getFeatureScore().map(_._2.toInt).sum

    booster
      .getFeatureScore()
      .map { case (feature, score) => {
        val featureIndex  = feature.substring(1).toInt
        val regulatorGene = trainingDMatrixGenes(featureIndex)

        val importance = if (normalize) score.toFloat / sum else score.toFloat

        Regulation(regulatorGene, targetGene, importance)
      }}
      .toSeq
      .sortBy(-_.importance)
  }

  def toDMatrix(m: SliceMatrix[Int, Int, Expression]) =
    new DMatrix(m.activeValuesIterator.toArray, m.rows, m.cols, 0f)

  def toDMatrix(csc: CSCMatrix[Expression]) =
    new DMatrix(csc.colPtrs.map(_.toLong), csc.rowIndices, csc.data, CSC)

  private def containedIn(targets: Set[Gene]): Gene => Boolean =
    if (targets.isEmpty)
      _ => true
    else
      targets.contains

}