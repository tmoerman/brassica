package org.tmoerman.brassica.util

import java.lang.System._
import java.util.concurrent.TimeUnit._

import scala.concurrent.duration.Duration

/**
  * @author Thomas Moerman
  */
object TimeUtils {

  def time[R](block: => R): (R, Duration) = {
    val start  = nanoTime
    val result = block
    val done   = nanoTime

    (result, Duration(done - start, NANOSECONDS))
  }

}