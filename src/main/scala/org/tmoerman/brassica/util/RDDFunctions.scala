package org.tmoerman.brassica.util

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RDDFunctions {

  implicit def pimp[T: ClassTag](rdd: RDD[T]): RDDFunctions[T] = new RDDFunctions(rdd)

}

class RDDFunctions[T: ClassTag](val rdd: RDD[T]) {

  def takeOrAll(n: Option[Int]): Array[T] = n.map(v => rdd.take(v)).getOrElse(rdd.collect)

}