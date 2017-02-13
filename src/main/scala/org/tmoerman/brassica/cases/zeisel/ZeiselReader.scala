package org.tmoerman.brassica.cases.zeisel

import breeze.linalg.{SparseVector => BreezeSparseVector}
import org.apache.spark.mllib.linalg.BreezeConversions._
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.reflect.ClassTag

/**
  * Read the Zeisel mRNA expression data a Spark SQL DataFrame.
  *
  * See: http://spark.apache.org/docs/latest/sql-programming-guide.html
  *
  * @author Thomas Moerman
  */
object ZeiselReader {

  type Index = Long
  type Line = (List[String], Index)

  private[zeisel] val NR_META_FEATURES  = 10
  private[zeisel] val EMPTY_LINE_INDEX  = NR_META_FEATURES
  private[zeisel] val FEAT_INDEX_OFFSET = NR_META_FEATURES + 1
  private[zeisel] val OBSERVATION_INDEX_OFFSET  = 2

  /**
    * @param spark The SparkSession.
    * @param file The Zeisel mRNA expression file name.
    * @return Returns a DataFrame of the Zeisel expression mRNA data.
    */
  def apply(spark: SparkSession, file: String, limit: Option[Int] = None): DataFrame = {
    val lines = rawLines(spark, file)

    val schema = parseSchema(lines)

    val nrFeatures = lines.count.toInt - NR_META_FEATURES

    val rows = parseRows(lines, nrFeatures, limit)

    spark.createDataFrame(rows, schema).na.fill(0)
  }

  /**
    * @param spark
    * @param file
    * @return Returns the raw lines without the empty line between meta and expression data.
    */
  private[zeisel] def rawLines(spark: SparkSession, file: String): RDD[Line] =
    spark
      .sparkContext
      .textFile(file)
      .map(_.split("\t").map(_.trim).toList)
      .zipWithIndex
      .filter(_._2 != EMPTY_LINE_INDEX)

  /**
    * Parse the Zeisel DataFrame schema.
    *
    * @param lines The RDD of raw lines.
    * @return Returns the schema StructType.
    */
  private[zeisel] def parseSchema(lines: RDD[Line]): StructType = {
    val features = StructField("expression", new VectorUDT(), nullable = false)

    val meta =
      (lines: @unchecked)
        .take(NR_META_FEATURES)
        .map {
          case (_ :: name :: _, 0l | 7l | 8l | 9l)      => StructField(name, StringType,  nullable = false)
          case (_ :: name :: _, 1l | 2l | 3l | 4l | 5l) => StructField(name, IntegerType, nullable = false)
          case (_ :: name :: _, 6l)                     => StructField(name, FloatType,   nullable = false)
        }
        .toList

    StructType(features :: meta)
  }

  /**
    * @param lines
    * @param nrExpressionFeatures
    * @param na
    * @return Returns row instances where the first entry in each row is the sparse expression vector,
    *         followed by the meta attributes.
    */
  private[zeisel] def parseRows(lines: RDD[Line],
                                nrExpressionFeatures: Int,
                                na: Option[Int] = Some(0),
                                takeCells: Option[Int] = None): RDD[Row] = {

    type ACC = (Array[Any], BreezeSparseVector[Double])

    def init(entry: (Any, Index)): ACC = {
      val meta     = Array.ofDim[Any](NR_META_FEATURES)
      val features = BreezeSparseVector.zeros[Double](nrExpressionFeatures)
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

        takeCells.map(n => result.take(n)).getOrElse(result)
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
      .map { case (meta, features) => Row.fromSeq(features.toMLLib :: meta.toList) }
  }

}