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

        val csc        = cscBroadcast.value
        val regulators = regulatorsBroadcast.value

        val fullDMatrix = toDMatrix(csc)

        it.flatMap(expressionByGene => {

          val input  = XGboostInput(expressionByGene, regulators, csc, fullDMatrix, params)

          val result = importanceScores(input) // TODO extract this to a "task" trait.

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
              matrixBuilder.add(cellIdx, geneIdx, value.toInt)
            }
        }

        Iterator(matrixBuilder.result)
      }
      .reduce(_ += _)
  }

  /**
    * @param expressionByGene
    * @param cscGenes The List of genes in the columns of the CSCMatrix.
    * @param csc The CSC matrix of gene expression values, only contains regulators.
    * @param fullDMatrix DMatrix built from the CSC Matrix.
    * @param params RegressionParams
    */
  case class XGboostInput(expressionByGene: ExpressionByGene,
                          cscGenes: List[Gene],
                          csc: CSCMatrix[Expression],
                          fullDMatrix: DMatrix,
                          params: RegressionParams)

  /**
    * @param input Input value object.
    * @return @return Calculate the importance scores for the regulators of the target gene.
    */
  def importanceScores(input: XGboostInput): Iterable[Regulation] = {
    import input._

    val targetGene = expressionByGene.gene
    val targetIsRegulator = cscGenes.contains(targetGene)
    println(s"-> $targetGene (regulator? $targetIsRegulator)")

    val regulatorsToTrainingDMatrixIndex =
      cscGenes
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene } // remove the target from the predictors

    // TODO move this to dedicated function

    resource
      .makeManagedResource(toTrainingDMatrix(input, targetIsRegulator, regulatorsToTrainingDMatrixIndex))(m => if (targetIsRegulator) m.delete())(Nil)
      .map(m => computeRegulations(m, targetGene, regulatorsToTrainingDMatrixIndex, params))
      .opt
      .get
  }

  private def toTrainingDMatrix(input: XGboostInput,
                                targetIsRegulator: Boolean,
                                regulatorsToTrainingDMatrixIndex: List[(Gene, GeneIndex)]) = {
    import input._

    lazy val withoutTargetDMatrix = toDMatrix(csc.apply(0 until csc.rows, regulatorsToTrainingDMatrixIndex.map(_._2)))
    val trainingDMatrix = if (targetIsRegulator) withoutTargetDMatrix else fullDMatrix

    trainingDMatrix.setLabel(expressionByGene.response)
    trainingDMatrix
  }

  def computeRegulations(trainingData: DMatrix,
                         targetGene: Gene,
                         regulatorsToTrainingDMatrixIndex: List[(Gene, GeneIndex)],
                         params: RegressionParams): Iterable[Regulation] = {
    import params._

    val booster = XGBoost.train(trainingData, boosterParams, nrRounds)

    // TODO refactor CV
    if (showCV) {
      val cv = XGBoost.crossValidation(trainingData, boosterParams, nrRounds, 10)
      val tuples =
        cv
          .map(_.split("\t").drop(1).map(_.split(":")(1).toFloat))
          .map{ case Array(train, test) => (train, test) }
          .zipWithIndex

      println(tuples.mkString(",\n"))
    }

    toScores(booster, targetGene, regulatorsToTrainingDMatrixIndex)
  }

  /**
    * @param booster The Booster instance.
    * @param targetGene The target gene.
    * @param regulatorsToTrainingDMatrixIndex List of (Gene, GeneIndex) tuples for the training DMatrix.
    * @return Returns a Seq of Regulation instances, ordered by importance DESC.
    */
  def toScores(booster: Booster,
               targetGene: Gene,
               regulatorsToTrainingDMatrixIndex: List[(Gene, GeneIndex)]): Iterable[Regulation] = {
    booster
      .getFeatureScore()
      .map { case (feature, importance) => {
        val featureIndex = feature.substring(1).toInt
        val regulatorGene = regulatorsToTrainingDMatrixIndex(featureIndex)._1

        Regulation(regulatorGene, targetGene, importance)
      }}
      .toSeq
      .sortBy(-_.importance)
  }

  def toDMatrix(m: SliceMatrix[Int, Int, Int]) =
    new DMatrix(m.activeValuesIterator.map(_.toFloat).toArray, m.rows, m.cols, 0f)

  def toDMatrix(csc: CSCMatrix[Int]) =
    new DMatrix(csc.colPtrs.map(_.toLong), csc.rowIndices, csc.data.map(_.toFloat), CSC)

  private def containedIn(targets: Set[Gene]): Gene => Boolean =
    if (targets.isEmpty)
      _ => true
    else
      targets.contains

}