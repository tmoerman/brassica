import org.aertslab.grnboost.Count

//val values = List(
//  7.07E+14,
//  4.81E+13,
//  3.65E+13,
//  1.98E+14,
//  1.78E+14,
//  7424315.0,
//  7252873.0,
//  6299353.0,
//  4545308.0,
//  1615836.0,
//  1442182.0,
//  1094521.0,
//  1072370.0,
//  752762.0,
//  623782.0,
//  541074.0,
//  504379.0,
//  421922.0,
//  415787.0).map(_.toFloat)
//
//val min = values.min
//val max = values.max
//
//val scaled = values.map(g => (g - min) / (max - min))
//
//val xScale = 10
//val precision = 0.001
//val c = DenseVector(values.length.toFloat * xScale, scaled.last)
//
//scaled
//  .sliding(2, 1)
//  .zipWithIndex
//  .toStream
//  .map{
//    case (va :: vb :: _, i) =>
//      val a = DenseVector(i.toFloat,      va)
//      val b = DenseVector(i.toFloat + 1f, vb)
//      val theta = angle(a, b, c)
//
//      ~=(theta, Pi, precision)
//
//    case _ => ??? // a.k.a. ka-boom
//  }


val ZERO: (Any, Count) = (null, 0)

val result =
  List(0, 0, 0, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1)
    .toStream
    .indexOfConsecutive(_ == 1, 5)

implicit class StreamFunctions[E](stream: Stream[E]) {

  type Idx = Int

  type T = ((E, Idx), Count)

  val ZERO: Option[T] = None

  def indexOfConsecutive(predicate: E => Boolean, minCount: Count) =
    stream
      .zipWithIndex
      .scanLeft(ZERO){ case (acc, (current, idx)) =>
        acc
          .map{ case ((prev, _), count) => ((current, idx), if (prev == current) count + 1 else 1) }
          .orElse(Some((current, 0), 1))
      }
      .dropWhile{
        case Some(((e, _), count)) => ! predicate(e) || count < minCount
        case None                    => true
      }
      .head

}