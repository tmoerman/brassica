package org.aertslab.grnboost.cases.zeisel

import java.util.concurrent.TimeUnit._

import org.scalatest.{FlatSpec, Matchers}
import org.aertslab.grnboost.GRNBoostSuiteBase
import org.aertslab.grnboost.Specs.Server
import org.aertslab.grnboost.util.TimeUtils

import scala.concurrent.duration.Duration

/**
  * @author Thomas Moerman
  */
class ZeiselBenchmarkInspector extends FlatSpec with GRNBoostSuiteBase with Matchers {

  "convert durations to seconds" should "run" taggedAs Server in {

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

}
