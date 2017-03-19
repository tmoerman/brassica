package org.tmoerman.brassica

import breeze.linalg.{CSCMatrix, SliceMatrix}
import ml.dmlc.xgboost4j.java.DMatrix.SparseType.CSC
import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.tmoerman.brassica.util.TimeUtils.{pretty, profile}

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
            expressionByGene: Dataset[ExpressionByGene],
            allGenes: List[Gene],
            candidateRegulators: Set[Gene],
            targets: Set[Gene] = Set.empty,
            params: RegressionParams = RegressionParams(),
            cellTop: Option[CellCount] = None,
            nrPartitions: Option[Int] = None): DataFrame = {

    val sc = spark.sparkContext

    val globalRegulatorIndex = toGlobalRegulatorIndex(allGenes, candidateRegulators)

    val (csc, duration) = profile {
      cscProducer.apply(globalRegulatorIndex)
    }

    println(s"constructing CSC matrix took ${pretty(duration)}")

    val cscBroadcast = sc.broadcast(csc)
    val globalRegulatorIndexBroadcast = sc.broadcast(globalRegulatorIndex)

    def isTarget(e: ExpressionByGene) = containedIn(targets)(e.gene)
    
    val rdd =
      nrPartitions
        .map(expressionByGene.rdd.repartition)
        .getOrElse(expressionByGene.rdd)
        .filter(isTarget)
        .cache

    val GRN = rdd.mapPartitions(it => {

      val csc = cscBroadcast.value
      val globalRegulatorIndex = globalRegulatorIndexBroadcast.value
      val full = toDMatrix(csc)

      it.flatMap(row => {
        val input = XGboostInput(row.gene, row.values.toArray.map(_.toFloat), globalRegulatorIndex, csc, full, params)

        val scores = importanceScores(input)

        if (it.isEmpty) {
          full.delete()
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
    * @param targetResponse The response vector of target gene.
    * @param globalRegulatorIndex Global index of the regulator genes.
    * @param csc The CSC matrix of gene expression values, only contains regulators.
    * @param fullDMatrix DMatrix built from the CSC Matrix.
    * @param params RegressionParams
    */
  case class XGboostInput(targetGene: String,
                          targetResponse: Array[Float],
                          globalRegulatorIndex: List[(Gene, GeneIndex)],
                          csc: CSCMatrix[Expression],
                          fullDMatrix: DMatrix,
                          params: RegressionParams)

  /**
    * @param input Input value object.
    * @return @return Calculate the importance scores for the regulators of the target gene.
    */
  def importanceScores(input: XGboostInput): Iterable[(Gene, Gene, Importance)] = {

    // TODO take this apart like hell

    import input._

    val targetIsRegulator = globalRegulatorIndex.exists{ case (gene, _) => gene == targetGene }

    println(s"-> $targetGene (regulator? $targetIsRegulator)")

    val regulatorCSCIndexTuples =
      globalRegulatorIndex
        .map(_._1)
        .zipWithIndex
        .filterNot { case (gene, _) => gene == targetGene } // remove the target from the predictors

    def toTrainingDMatrix = {
      lazy val withoutTargetDMatrix = toDMatrix(csc.apply(0 until csc.rows, regulatorCSCIndexTuples.map(_._2)))
      val trainingDMatrix = if (targetIsRegulator) withoutTargetDMatrix else fullDMatrix
      trainingDMatrix.setLabel(targetResponse)

      trainingDMatrix
    }

    def performXGBoost(trainingData: DMatrix) = {
      import params.{boosterParams, normalize, nrRounds}

      val booster = XGBoost.train(trainingData, boosterParams, nrRounds)

      // TODO refactor CV

      /*
      val cv = XGBoost.crossValidation(trainingData, boosterParams, nrRounds, 10)
      val tuples =
        cv
          .map(_.split("\t").drop(1).map(_.split(":")(1).toFloat))
          .map{ case Array(train, test) => (train, test) }
      println(tuples.mkString(",\n"))
      */

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
      .makeManagedResource(toTrainingDMatrix)(m => if (targetIsRegulator) m.delete())(Nil)
      .map(performXGBoost)
      .opt
      .get
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

  /**
    * @param allGenes The ordered List of all genes.
    * @param candidateRegulators The Set of candidate regulator genes.
    * @return Returns a List[(Gene -> GeneIndex)], mapping the genes present in the List of
    *         candidate regulators to their index in the complete gene List.
    */
  def toGlobalRegulatorIndex(allGenes: List[Gene], candidateRegulators: Set[Gene]): List[(Gene, GeneIndex)] =
    allGenes
      .zipWithIndex
      .filter{ case (gene, _) => candidateRegulators.contains(gene) }

}