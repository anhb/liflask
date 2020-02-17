package com.anhb.liflask.sqlutils.functions

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.anhb.liflask.sqlutils.logs.LoggingFunc
import org.apache.spark.sql.functions.{col, max}

class ReadWriteFunc(spark: SparkSession) extends LoggingFunc{

  def readDFByMaxDate(path: String, format: String, date_col: String, listCols: List[String] = List("*")): DataFrame = {
    try {
      spark.read.format(format).load(path).selectExpr(listCols: _*).filter(col(date_col) === spark.read.format(format).load(path).select(date_col).agg(max(date_col)).collect().head.getDate(0).toString)
    } catch {
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }

  def readDFWithoutOptions(path: String, format: String = "parquet", list_cols: List[String] = List("*")): DataFrame = {
    try {
      spark.read.format(format).load(path).selectExpr(list_cols: _*)
    }catch {
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }

  def dynamicallyRepartitionByCore(spark: SparkSession, df: DataFrame): Int = spark.sparkContext.defaultParallelism * 4

}
