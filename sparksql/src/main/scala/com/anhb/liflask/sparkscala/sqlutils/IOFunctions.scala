package com.anhb.liflask.sparkscala.sqlutils

import com.anhb.liflask.sparkscala.utils.variablesLog
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, max}

//class IOFunctions(sparkSession: SparkSession) extends SparkInit{
class IOFunctions(sparkSession: SparkSession) extends variablesLog {

  def inputDataframeByMaxDate(path: String, dateCol: String): DataFrame = {
    try {
      var tableInput = sparkSession.read.format("parquet").option("header", "true")
        .load(path)
      tableInput = tableInput.filter(col(dateCol) === tableInput.agg(max(col(dateCol))).collect().head.getDate(0).toString)
      tableInput
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }

  def writeTable(writingTable: DataFrame, pathToWrite: String, format: String = "csv", repartitions: Int = 1, headerToF: String = "true", quotes: Boolean = false): Int = {
    try {
      logger.info("Writing "+ format + " File")
      writingTable
        .repartition(repartitions)
        .write.mode(SaveMode.Append)
        .option("header", headerToF)
        .option("quoteAll", quotes)
        .format(format)
        .save(pathToWrite)
      logger.info(format + " was wrote")
      0
    }catch {
      case e: Exception => {
        logger.error(excepLog)
        throw e
      }
    }
  }




}
