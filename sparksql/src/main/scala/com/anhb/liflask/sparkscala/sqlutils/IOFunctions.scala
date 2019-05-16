package com.anhb.liflask.sparkscala.sqlutils

import com.anhb.liflask.sparkscala.process.SparkInit
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max}

object IOFunctions extends SparkInit{

  def inputDataframeByMaxDate(path: String, dateCol: String): DataFrame = {
    try {
      var tableInput = sparkSQLInit().sqlContext.read.format("parquet").option("header", "true")
        .load(path)
      tableInput = tableInput.filter(col(dateCol) === tableInput.agg(max(col(dateCol))).collect().head.getDate(0).toString)
      tableInput
    }catch {
      case e: Exception => {
        logger.info(s"Exception Found: ")
        throw e
      }
    }
  }



}
