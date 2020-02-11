package com.anhb.liflask.sqlutils.logs

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging

trait LoggingVar extends LazyLogging {

  lazy val log_exception: Unit = logger.error("[ Exception Found ] ==> ")
  lazy val start_time: Long = System.nanoTime()
  lazy val total_time: Long = TimeUnit.SECONDS.convert(System.nanoTime() - start_time, TimeUnit.NANOSECONDS)
  lazy val log_greeting: Unit = logger.info("|---------- Init Process ----------|")
  lazy val log_custom_greeting: String => Unit = greeting => logger.info(s"|---------- $greeting ----------|")
  lazy val log_time_end_process: Long => Unit = total_time_seconds => logger.info(
    s"|--- Success Process [OK] Total Time =>  Hrs: "
      + total_time_seconds / 3600 + " Min: " + total_time_seconds / 60 + " Sec: " + total_time_seconds % 60 + " ---|")

}
