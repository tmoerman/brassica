package org.tmoerman.brassica.cases.zeisel

import java.lang.Math.min

import breeze.linalg.{CSCMatrix, SparseVector => BSV}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.tmoerman.brassica._
import org.tmoerman.brassica.cases.DataReader
import org.apache.spark.ml.linalg.BreezeMLConversions._
import org.apache.spark.ml.linalg.SparseVector

import scala.reflect.ClassTag

/**
  * Read the Zeisel mRNA expression data a Spark SQL DataFrame.
  *
  * See: http://spark.apache.org/docs/latest/sql-programming-guide.html
  *
  * @author Thomas Moerman
  */
object ZeiselReader extends DataReader {

  type Line = (List[String], Index)

  // use these for test purposes, not to be hard coded in parser logic
  private[zeisel] val ZEISEL_CELL_COUNT = 3005
  private[zeisel] val ZEISEL_GENE_COUNT = 19972

  private[zeisel] val NR_META_FEATURES  = 10
  private[zeisel] val EMPTY_LINE_INDEX  = NR_META_FEATURES
  private[zeisel] val FEAT_INDEX_OFFSET = NR_META_FEATURES + 1
  private[zeisel] val OBSERVATION_INDEX_OFFSET  = 2

  /**
    * @param spark The SparkSession.
    * @param raw The raw Zeisel file.
    * @return Returns a tuple:
    *         - DataFrame of the Zeisel expression mRNA data with schema
    *         - Gene list
    */
  def apply(spark: SparkSession, raw: Path): (DataFrame, List[Gene]) =
    apply(spark, rawLines(spark, raw).cache)

  /**
    * @param spark The SparkSession.
    * @param raw The raw Zeisel file path.
    * @return Returns the raw lines without the empty line between meta and expression data.
    */
  private[zeisel] def rawLines(spark: SparkSession, raw: Path): RDD[Line] =
    spark
      .sparkContext
      .textFile(raw)
      .map(_.split("\t").map(_.trim).toList)
      .zipWithIndex
      .filter(_._2 != EMPTY_LINE_INDEX)

  /**
    * @param spark The SparkSession.
    * @param lines The (cached) lines parsed from the raw Zeisel file.
    * @return Returns a tuple:
    *         - DataFrame of the Zeisel expression mRNA data with schema
    *         - Gene list
    */
  def apply(spark: SparkSession, lines: RDD[Line]): (DataFrame, List[Gene]) = {
    val genes = readGenes(lines)

    val schema = parseSchema(lines)

    val nrExpressionFeatures = lines.count.toInt - NR_META_FEATURES

    val rows = parseRows(lines, nrExpressionFeatures)

    val df =
      spark
        .createDataFrame(rows, schema)
        .na.fill(0) // TODO is this necessary?

    (df, genes)
  }

  /**
    * @param lines The RDD of lines.
    * @return Returns the List of Gene names.
    */
  private[zeisel] def readGenes(lines: RDD[Line]): List[Gene] =
    lines
      .filter(_._2 >= FEAT_INDEX_OFFSET)
      .map(_._1.head)
      .collect
      .toList

  /**
    * @param spark The SparkSession.
    * @param parquetFile The Zeisel parquet file.
    * @param rawFile The raw Zeisel data file.
    * @return Returns the Zeisel gene expression DataFrame from a parquet file.
    */
  def fromParquet(spark: SparkSession, parquetFile: String, rawFile: String): (DataFrame, List[Gene]) = {
    val df = spark.read.parquet(parquetFile).cache

    val genes = readGenes(spark, rawFile)

    (df, genes)
  }

  /**
    * @param spark The SparkSession.
    * @param raw The raw Zeisel file path.
    * @return Returns the List of Gene names.
    */
  private[zeisel] def readGenes(spark: SparkSession, raw: Path): List[Gene] = {

    // left of the separator Char
    def leftOf(c: Char)(s: String) = s.splitAt(s.indexOf(c))._1

    spark
      .sparkContext
      .textFile(raw)
      .zipWithIndex
      .filter{ case (_, idx) => idx >= FEAT_INDEX_OFFSET }
      .map{ case (s, _) => leftOf('\t')(s)}
      .collect
      .toList
  }

  /**
    * @param lines The RDD of raw lines.
    * @return Returns the schema StructType.
    */
  private[zeisel] def parseSchema(lines: RDD[Line]): StructType = {

    def clean(name: String) = name.replace(' ', '_').replace("#", "count")

    val meta =
      lines
        .take(NR_META_FEATURES)
        .map {
          case (_ :: name :: _, 0l | 7l | 8l | 9l)      => StructField(clean(name), StringType,  nullable = false)
          case (_ :: name :: _, 1l | 2l | 3l | 4l | 5l) => StructField(clean(name), IntegerType, nullable = false)
          case (_ :: name :: _, 6l)                     => StructField(clean(name), FloatType,   nullable = false)
          case _ => ???
        }
        .toList

    StructType(EXPRESSION_STRUCT_FIELD :: meta)
  }

  /**
    * @param lines The RDD of lines.
    * @param expressionVectorLength The length of a gene expression vector.
    * @param na The value to consider as N/A.
    * @return Returns row instances where the first entry in each row is the sparse expression vector,
    *         followed by the meta attributes.
    */
  private[zeisel] def parseRows(lines: RDD[Line],
                                expressionVectorLength: Count,
                                na: Option[Int] = Some(0), // TODO necessary?
                                nrCells: Option[Int] = None): RDD[Row] = {

    type ACC = (Array[Any], BSV[Double])

    def init(entry: (Any, Index)): ACC = {
      val meta     = Array.ofDim[Any](NR_META_FEATURES)
      val features = BSV.zeros[Double](expressionVectorLength)
      val acc      = (meta, features)

      update(acc, entry)
    }

    def update(acc: ACC, entry: (Any, Index)): ACC = acc match { case (meta, features) =>
      (entry: @unchecked) match {
        case (v, metaIdx) if metaIdx < NR_META_FEATURES => meta.update(metaIdx.toInt, v)
        case (v: Double, featIdx) =>
          features.update(featIdx.toInt - FEAT_INDEX_OFFSET, v)
      }

      acc
    }

    def merge(a: ACC, b: ACC) = (a, b) match { case ((a_meta, a_features), (b_meta, b_features)) =>
      val meta = (a_meta zip b_meta).map{
        case (l, null) => l
        case (null, r) => r
        case _         => null
      }

      val features = a_features += b_features

      (meta, features)
    }

    implicit class PimpCols[T: ClassTag](list: List[T]) {
      def prep: List[T] = {
        val result = list.drop(OBSERVATION_INDEX_OFFSET)

        nrCells.map(n => result.take(n)).getOrElse(result)
      }
    }

    lines
      .flatMap {
        case (cols, metaIdx @ (0l | 7l | 8l | 9l))      => cols.prep.zipWithIndex.map     { case (v, cellIdx) => (cellIdx, (v,         metaIdx)) }
        case (cols, metaIdx @ (1l | 2l | 3l | 4l | 5l)) => cols.prep.zipWithIndex.map     { case (v, cellIdx) => (cellIdx, (v.toInt,   metaIdx)) }
        case (cols, metaIdx @ 6l)                       => cols.prep.zipWithIndex.map     { case (v, cellIdx) => (cellIdx, (v.toFloat, metaIdx)) }
        case (cols, featIdx)                            => cols.prep.zipWithIndex.flatMap { case (v, cellIdx) =>
          if (na.contains(v.toInt))
            Seq.empty
          else
            Seq((cellIdx, (v.toDouble, featIdx)))
        }}
      .combineByKey(init, update, merge)
      .sortByKey()
      .values
      .map { case (meta, features) => Row.fromSeq(features.ml :: meta.toList) }
  }

  /**
    * @param spark The SparkSession.
    * @param raw The raw Zeisel file.
    * @return Returns a DataFrame:
    *
    *         | gene | expression |
    */
  def readExpressionByGene(spark: SparkSession, raw: Path): DataFrame =
    readExpressionByGene(spark, rawLines(spark, raw))

  /**
    * @param spark
    * @param lines
    * @return Returns a DataFrame:
    *         | gene | expression |
    */
  def readExpressionByGene(spark: SparkSession, lines: RDD[Line]): DataFrame = {
    val geneColumnVectorTuples =
      parseExpressionByGene(lines)
        .map{ case (gene, values) => Row(gene, values.ml) }

    val schema =
      StructType(
        StructField(GENE, StringType) ::
        new AttributeGroup(VALUES).toStructField :: Nil)

    spark.createDataFrame(geneColumnVectorTuples, schema)
  }

  /**
    * @param df
    * @param cellTop
    * @param onlyGeneIndices
    * @return Returns a CSCMatrix parsed from the Zeisel DataFrame.
    */
  def toCSCMatrix(df: DataFrame,
                  cellTop: Option[CellCount] = None,
                  onlyGeneIndices: Seq[GeneIndex],
                  nrCells: CellCount = ZEISEL_CELL_COUNT,
                  nrGenes: GeneCount = ZEISEL_GENE_COUNT): CSCMatrix[Expression] = {

    val predictorSlicer =
      new VectorSlicer()
        .setInputCol(EXPRESSION)
        .setOutputCol(REGULATORS)
        .setIndices(onlyGeneIndices.toArray)

    val regulators =
      Some(df)
        .map(predictorSlicer.transform)
        .map(_.select(REGULATORS))
        .get

    val cellDim = cellTop.map(min(_, nrCells)).getOrElse(nrCells)
    val geneDim = onlyGeneIndices.size

    regulators
      .rdd
      .zipWithIndex // order of cell observations might be different...
      .mapPartitions { it =>
        val matrixBuilder = new CSCMatrix.Builder[Expression](rows = cellDim, cols = geneDim)

        it.foreach{ case (row, cellIdx) =>
          row
            .getAs[SparseVector](0)
            .br
            .activeIterator
            .foreach { case (geneIdx, value) => matrixBuilder.add(cellIdx.toInt, geneIdx, value.toInt) }
        }

        Iterator(matrixBuilder.result) }
      .reduce(_ += _)
  }

  def readCSCMatrix(lines: RDD[Line],
                    cellTop: Option[CellCount] = None,
                    onlyGeneIndices: Option[Seq[GeneIndex]] = None,
                    nrCells: CellCount = ZEISEL_CELL_COUNT,
                    nrGenes: GeneCount = ZEISEL_GENE_COUNT): CSCMatrix[Expression] = {

    val cellDim = cellTop.map(min(_, nrCells)).getOrElse(nrCells)
    val geneDim = onlyGeneIndices.map(_.size).getOrElse(nrGenes)

    // TODO finish me
//    val genePredicate = onlyGeneIndices.map(_.toSet)
//    val reindex: GeneIndex => GeneIndex = onlyGeneIndices.map(_.zipWithIndex.toMap).getOrElse(identity)

    lines
      .filter{ case (_, lineIdx) => lineIdx >= NR_META_FEATURES } // get rid of meta field values
      .map{ case (v, lineIdx) => (v, lineIdx - NR_META_FEATURES)} // remap to GeneIndex

      .mapPartitions{ it =>
        val matrixBuilder = new CSCMatrix.Builder[Expression](rows = cellDim, cols = geneDim)

        it.foreach {
          case (_ :: _ :: expressionByGene, geneIdx) =>

            expressionByGene
              .zipWithIndex
              .foreach { case (value, cellIdx) =>
                matrixBuilder.add(cellIdx, geneIdx.toInt, value.toInt)
              }

          case _ => Unit
        }

        Iterator(matrixBuilder.result) }
      .reduce(_ += _)
  }


  /**
    * @param lines RDD of raw lines.
    * @return Returns an RDD of (gene, expressionVectorByGene)
    */
  private[zeisel] def parseExpressionByGene(lines: RDD[Line]): RDD[(Gene, BSV[Double])] =
    lines
      .filter{ case (_, lineIdx) => lineIdx >= NR_META_FEATURES } // get rid of meta field values
      .map {
        case (gene :: _ :: values, _) => (gene, expressionByGene(values))
        case _ => ???
      }

  private[zeisel] def expressionByGene(values: List[String]): BSV[Double] = {
    val tuples =
      values
        .zipWithIndex
        .flatMap { case (value, cellIdx) => if (value == "0") Nil else (cellIdx, value.toDouble) :: Nil }

    BSV(values.length)(tuples: _*)
  }

}