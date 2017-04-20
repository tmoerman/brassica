package org.tmoerman.brassica.cases.zeisel

import java.util.concurrent.TimeUnit._

import org.scalatest.{Matchers, FlatSpec}
import org.tmoerman.brassica.GRNBoostSuiteBase
import org.tmoerman.brassica.util.TimeUtils

import scala.concurrent.duration.Duration

/**
  * @author Thomas Moerman
  */
class ZeiselBenchmarkInspector extends FlatSpec with GRNBoostSuiteBase with Matchers {

  "convert durations to seconds" should "run" in {

    val durations =
    (1,     (0, 0, 19))  ::
    (5,     (0, 0, 11))  ::
    (10,    (0, 0, 12))  ::
    (25,    (0, 0, 18))  ::
    (100,   (0, 0, 43))  ::
    (250,   (0, 1, 33))  ::
    (1000,  (0, 5, 54))  ::
    (2500,  (0, 14, 29)) ::
    (10000, (0, 52, 44)) ::
    (19972, (1, 40, 36)) :: Nil

    val s =
    durations
      .map{ case (nr, (hr, min, sec)) =>
        val d = Duration(hr,  HOURS) + Duration(min, MINUTES) + Duration(sec, SECONDS)

        Map("nr_targets" -> nr,
            "duration" -> d.toSeconds,
            "pretty" -> TimeUtils.pretty(d)) }
      .mkString("\n")

    println(s)
  }

  "inspection" should "run" in {
    import spark.implicits._

    val path = "src/test/resources/out/zeisel_t19972_r100"

    val df = spark.read.parquet(path)

    df.filter($"target_name" === "Gad1").orderBy($"importance".desc).show()

    println(path)
  }

}
