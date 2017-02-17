package org.tmoerman.brassica.util

import java.lang.System._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit._

import scala.concurrent.duration.Duration

/**
  * @author Thomas Moerman
  */
object TimeUtils {

  def profile[R](block: => R): (R, Duration) = {
    val start  = nanoTime
    val result = block
    val done   = nanoTime

    (result, Duration(done - start, NANOSECONDS))
  }

  def timestamp = new SimpleDateFormat("yyyy.MM.dd_HH.mm.ss").format(Calendar.getInstance.getTime)

}