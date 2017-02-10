package org.tmoerman.brassica.zeisel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructField

import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
  * @author Thomas Moerman
  */
object ZeiselReaderDF {

  /**
    * Read the Zeisel mRNA expression data a Spark SQL DataFrame.
    *
    * See: http://spark.apache.org/docs/latest/sql-programming-guide.html
    *
    * @param spark The SparkSession.
    * @param file The Zeisel mRNA expression file name.
    * @return Returns a DataFrame of the Zeisel expression mRNA data.
    */
  def apply(spark: SparkSession, file: String, limit: Option[Int] = None): DataFrame = {
    val lines = rawLines(spark, file)

    val schema = parseSchema(lines)

    val rows = parseRows(lines, schema.size, limit)

    spark.createDataFrame(rows, schema).na.fill(0)
  }

  type Index = Long
  type Line = (List[String], Index)

  private[zeisel] val EMPTY_LINE_INDEX = 10

  private[zeisel] def rawLines(spark: SparkSession, file: String): RDD[Line] =
    spark
      .sparkContext
      .textFile(file)
      .map(_.split("\t").map(_.trim).toList)
      .zipWithIndex
      .filter(_._2 != EMPTY_LINE_INDEX)

  val META = new MetadataBuilder().putBoolean("gene", false).build

  def gene(backspin: Long) = new MetadataBuilder().putBoolean("gene", true).putLong("backspin", backspin).build

  /**
    * Parse the Zeisel DataFrame schema.
    *
    * @param lines The RDD of raw lines.
    * @return Returns the schema StructType.
    */
  private[zeisel] def parseSchema(lines: RDD[Line]): StructType = {
    val fields =
      (lines: @unchecked)
        .map {
          case (_ :: name :: _, 0l | 7l | 8l | 9l)      => StructField(name, StringType,  nullable = false, META)
          case (_ :: name :: _, 1l | 2l | 3l | 4l | 5l) => StructField(name, IntegerType, nullable = false, META)
          case (_ :: name :: _, 6l)                     => StructField(name, FloatType,   nullable = false, META)
          case (name :: b :: _, _)                      => StructField(name, IntegerType, nullable = true,  gene(b.toLong)) }
        .collect
        .toList

    StructType(fields)
  }

  /**
    * Parse the Zeisel DataFrame rows.
    *
    * @param lines The RDD of raw lines.
    * @param nrFeatures The number of features for each cell.
    * @param na The expression value that will be treated as N/A.
    * @return Returns an RDD of Row instances, that will be used to construct the DataFrame.
    */
  private[zeisel] def parseRows(lines: RDD[Line],
                                nrFeatures: Int,
                                na: Option[Int] = Some(0),
                                limit: Option[Int] = None): RDD[Row] = {
    type ACC = Array[Any]

    def init(entry: (Any, Index)): ACC = entry match { case (v, i) =>
      val acc = Array.ofDim[Any](nrFeatures)
      acc.update(i.toInt, v)
      acc
    }

    def insert(acc: ACC, t: (Any, Index)): ACC = t match { case (v, i) =>
      acc.update(i.toInt, v)
      acc
    }

    def merge(a: ACC, b: ACC): ACC = (a zip b).map {
      case (l, null) => l
      case (null, r) => r
      case _         => null
    }

    lines
      .flatMap {
        case (cols, feat @ (0l | 7l | 8l | 9l))      => cols.drop(2).zipWithIndex.map { case (v, cell) => (cell, (v,         feat)) }
        case (cols, feat @ (1l | 2l | 3l | 4l | 5l)) => cols.drop(2).zipWithIndex.map { case (v, cell) => (cell, (v.toInt,   feat)) }
        case (cols, feat @ 6l)                       => cols.drop(2).zipWithIndex.map { case (v, cell) => (cell, (v.toFloat, feat)) }
        case (cols, feat) => cols.drop(2).takeOrAll(limit).zipWithIndex.flatMap { case (v, cell) =>
          if (na.contains(v.toInt))
            Seq.empty
          else
            Seq((cell, (v.toInt,   feat-1))) }}
      .combineByKey(init, insert, merge)
      .sortByKey()
      .values
      .map(a =>
        Row.fromSeq(a)
      )
  }

  implicit class Extra[T: ClassTag](list: List[T]) {
    def takeOrAll(n: Option[Int]): List[T] = n.map(v => list.take(v)).getOrElse(list)
  }

}