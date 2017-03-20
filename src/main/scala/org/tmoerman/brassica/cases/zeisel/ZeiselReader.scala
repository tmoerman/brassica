package org.tmoerman.brassica.cases.zeisel

import breeze.linalg.{CSCMatrix, SparseVector => BSV}
import org.apache.spark.ml.linalg.BreezeMLConversions._
import org.apache.spark.ml.linalg.{Vectors, Vector => MLVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.tmoerman.brassica._
import org.tmoerman.brassica.cases.DataReader

import scala.reflect.ClassTag

/**
  * Read the Zeisel mRNA expression data a Spark SQL DataFrame.
  *
  * See: http://spark.apache.org/docs/latest/sql-programming-guide.html
  *
  * @author Thomas Moerman
  */
object ZeiselReader extends DataReader {

  // TODO remove all the obsolete crap

  /**
    * Type representing a Line of the Zeisel mRNA expression file.
    * The raw file has row ~ features, and cols ~ values per cell
    *
    * A Line consists of a feature and the values for that feature across cells.
    */
  private[zeisel] type Line = (List[String], Index)

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
      .zipWithIndex
      .map{ case (string, idx) =>
        val split = string.split("\t").map(_.trim).toList
        (split, idx) }
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
    * @param ds The Dataset of ExpressionByGene instances.
    * @param regulators The ordered List of regulators.
    * @param nrCells The nr of cells = nr of row in the CSCMatrix
    * @return Returns a CSCMatrix of expression values.
    */
  def toCSCMatrix(ds: Dataset[ExpressionByGene],
                  regulators: List[Gene],
                  nrCells: CellCount = ZEISEL_CELL_COUNT): CSCMatrix[Expression] = {

    val nrGenes = regulators.size

    val regulatorIndexMap = regulators.zipWithIndex.toMap

    def isPredictor(gene: Gene) = regulatorIndexMap.contains(gene)
    def cscIndex(gene: Gene) = regulatorIndexMap.apply(gene)

    ds.rdd
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
      .reduce(_ + _)
  }

  private[zeisel] def toExpressionByGene(line: Line): Option[ExpressionByGene] =
    Some(line)
      .filter(_._2 >= NR_META_FEATURES)
      .map{
        case (gene :: _ :: values, _) => ExpressionByGene(gene, toExpressionVector(values))
        case _                        => ???
      }

  private[zeisel] def toExpressionVector(values: List[String]): MLVector = {
    val elements =
      values
        .zipWithIndex
        .filterNot(_._1 == "0")
        .map { case (value, cellIdx) => (cellIdx, value.toDouble) }

    Vectors.sparse(values.length, elements)
  }

}