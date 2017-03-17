package org.tmoerman.brassica

import breeze.linalg.{CSCMatrix, SliceMatrix}
import ml.dmlc.xgboost4j.java.DMatrix.SparseType.CSC
import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @author Thomas Moerman
  */
object ScenicPipeline {

  /**
    * @param spark The SparkSession.
    * @param cscProducer Function that constructs a CSCMatrix[Expression] in (rows = cells, cols = genes).
    * @param expressionByGene A DataFrame containing the expression values by gene.
    *                         | gene | value |
    * @param allGenes The ordered List of all genes.
    * @param candidateRegulators The Set of candidate regulators (TF).
    *                            The term "candidate" is used to imply that not all these regulators are expected
    *                            to be present in the specified List of all genes.
    * @param targets A Set of target genes for which we wish to infer the important regulators.
    *                If empty Set is specified, this is interpreted as: target genes = all genes.
    * @param params The XGBoost regression parameters.
    * @param cellTop Optional nr of top cells to consider for the regression.
    *                All cells are used if None is specified.
    * @param nrPartitions Optional technical parameter for defining the nr. of Spark partitions to use.
    * @return Returns a DataFrame with schema:
    *         | regulator_name | target_name | importance |
    */
  def apply(spark: SparkSession,
            cscProducer: List[(Gene, GeneIndex)] => CSCMatrix[Expression],
            expressionByGene: DataFrame,
            allGenes: List[Gene],
            candidateRegulators: Set[Gene],
            targets: Set[Gene] = Set.empty,
            params: RegressionParams = RegressionParams(),
            cellTop: Option[CellCount] = None,
            nrPartitions: Option[Int] = None): DataFrame = {

    val sc = spark.sparkContext

    val globalRegulatorIndex = toGlobalRegulatorIndex(allGenes, candidateRegulators)

    val csc = cscProducer.apply(globalRegulatorIndex)
    val cscBroadcast = sc.broadcast(csc)

    val globalRegulatorIndexBroadcast = sc.broadcast(globalRegulatorIndex)

    def isTarget(row: Row) = containedIn(targets)(row.gene)

    val rdd =
      nrPartitions
        .map(expressionByGene.rdd.repartition)
        .getOrElse(expressionByGene.rdd)
        .filter(isTarget)
        .cache

    assert(rdd.count == targets.size, "kloewete!")

    val GRN = rdd.mapPartitions(it => {

      val csc = cscBroadcast.value
      val index = globalRegulatorIndexBroadcast.value
      val unsliced = toDMatrix(csc)

      it.flatMap(row => {
        val scores = importanceScores(row.gene, row.data, index, csc, unsliced, params)

        if (it.isEmpty) {
          unsliced.delete()
        }

        scores
      })
    })

    spark
      .createDataFrame(GRN)
      .toDF(REGULATOR_NAME, TARGET_NAME, IMPORTANCE)
  }

  /**
    * @param targetGene The target gene.
    * @param response The response vector of target gene.
    * @param globalRegulatorIndex Global index of the regulator genes.
    * @param csc The CSC matrix of gene expression values, only contains regulators.
    * @param params Parameters for the regressions.
    * @return Calculate the importance scores for the regulators of the target gene.
    */
  def importanceScores(targetGene: String,
                       response: Array[Float],
                       globalRegulatorIndex: List[(Gene, GeneIndex)],
                       csc: CSCMatrix[Expression],
                       unsliced: DMatrix,
                       params: RegressionParams): Iterable[(Gene, Gene, Importance)] = {

    val targetIsRegulator = globalRegulatorIndex.exists{ case (gene, _) => gene == targetGene }

    val regulatorCSCIndexTuples =
      globalRegulatorIndex
        .map(_._1)
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene } // remove the target from the predictors

    def toTrainingData = {
      lazy val sliced = toDMatrix(csc.apply(0 until csc.rows, regulatorCSCIndexTuples.map(_._2)))
      val trainingData = if (targetIsRegulator) sliced else unsliced
      trainingData.setLabel(response)
      trainingData
    }

    def performXGBoost(trainingData: DMatrix) = {
      import params._

      val booster = XGBoost.train(trainingData, boosterParams, nrRounds)

      val sum = booster.getFeatureScore().values.map(_.toInt).sum

      val scores =
        booster
          .getFeatureScore()
          .map { case (feature, score) => {
            val featureIndex = feature.substring(1).toInt
            val (regulatorGene, _) = regulatorCSCIndexTuples(featureIndex)
            val importance = if (normalize) score.toFloat / sum else score.toFloat

            (regulatorGene, targetGene, importance)}}
          .toSeq
          .sortBy(- _._3)

      scores
    }

    resource
      .makeManagedResource(toTrainingData)(m => if (targetIsRegulator) m.delete())(Nil)
      .map(performXGBoost)
      .opt
      .get
  }

  def toDMatrix(m: SliceMatrix[Int, Int, Int]) =
    new DMatrix(m.activeValuesIterator.map(_.toFloat).toArray, m.rows, m.cols, 0f)

  def toDMatrix(csc: CSCMatrix[Int]) =
    new DMatrix(csc.colPtrs.map(_.toLong), csc.rowIndices, csc.data.map(_.toFloat), CSC)

  private implicit class PimpRow(row: Row) {
    def gene: String = row.getAs[String](GENE)
    def data: Array[Float] = row.getAs[SparseVector](VALUES).toArray.map(_.toFloat)
  }

  private def containedIn(targets: Set[Gene]): Gene => Boolean =
    if (targets.isEmpty)
      (_ => true)
    else
      targets.contains _

  /**
    * @param allGenes The List of all genes in the data set.
    * @param candidateRegulators The Set of candidate regulator genes.
    * @return Returns a List[(Gene -> GeneIndex)], mapping the genes present in the List of
    *         candidate regulators to their index in the complete gene List.
    */
  def toGlobalRegulatorIndex(allGenes: List[Gene], candidateRegulators: Set[Gene]): List[(Gene, GeneIndex)] = {
    assert(candidateRegulators.nonEmpty)

    val isRegulator = candidateRegulators.contains _

    allGenes
      .zipWithIndex
      .filter{ case (gene, _) => isRegulator(gene) }
  }

}