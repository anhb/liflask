package com.anhb.liflask.sqlutils.logs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SizeEstimator

trait LoggingFunc extends LoggingVar{

  private val exit_code: Int = 0

  def initProcess(spark: SparkSession): Unit = {
    try{
      start_time
      log_greeting
      spark.sqlContext.clearCache()
    }catch{
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }

  def endingProcess: Int = {
    try {
      log_time_end_process(total_time)
      exit_code
    }catch {
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }

  protected def loggingGetSizeDF(df:DataFrame): Unit = {
    try {
      val size_df: Long = SizeEstimator.estimate(df)
      val n = 1000
      var s = ""
      val kb = size_df / n
      val mb = kb / n
      val gb = mb / n
      val tb = gb / n
      if (size_df < n) {
        size_df + " Bytes"
      }
      else if (size_df >= n && size_df < (n * n)) {
        s = f"$kb%.2f" + " KB"
      }
      else if (size_df >= (n * n) && size_df < (n * n * n)) {
        s = f"$mb%.2f" + " MB"
      }
      else if (size_df >= (n * n * n) && size_df < (n * n * n * n)) {
        s = f"$gb%.2f" + " GB"
      }
      else if (size_df >= (n * n * n * n)) {
        s = f"$tb%.2f" + " TB"
      }
      logger.info("Approximately DataFrame Size ==> " + s)
    }catch {
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }

  def logVariablesControlM(ListVars: List[Any], ListNames: List[String]): Unit ={
    try {
      val iter: Int = 0
      logger.info("=============================================")
      for (i <- iter until ListVars.length) logger.info("====> " + ListNames(i) + ":  " + ListVars(i))
      logger.info("=============================================")
    }catch {
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }

}
