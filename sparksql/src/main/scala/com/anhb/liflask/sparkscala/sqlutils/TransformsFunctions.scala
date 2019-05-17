package com.anhb.liflask.sparkscala.sqlutils

import com.anhb.liflask.sparkscala.process.SparkInit
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{sum, col}

object TransformsFunctions extends SparkInit {

  def sumByClient(table: DataFrame, columnGroupBy: String, sumCols: List[String]): DataFrame ={
    try{
      val sums = sumCols.map(colName => sum(colName).as("sum_" + colName))
      val tableDF = table.groupBy(columnGroupBy).agg(sums.head, sums.tail:_*)
      tableDF
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }

  def selectMul(tableDF: DataFrame, listCols: List[String]): DataFrame = {
    try {
      val selectFil = tableDF.select(listCols.head, listCols.tail: _*)
      selectFil
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }

  def joinMul(tables: List[DataFrame], cols: List[String]): DataFrame = {
    try {
      val tableJoin = tables.reduce(_.join(_, Seq(cols: _*), "left"))
      tableJoin
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }

  def fillNullValues(tableDF: DataFrame, listCols: List[String], fillVal: Int): DataFrame = {
    try {
      val fillTable = tableDF.na.fill(fillVal, Seq(listCols: _*))
      fillTable
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }

  def unionTables(tables: List[DataFrame]): DataFrame = {
    try {
      val unionTable = tables.reduce(_.union(_))
      unionTable
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }

  def mulFunction(table: DataFrame, val1: String, val2: String, newNameCol: String): DataFrame = {
    try {
      val mulDataframe = table.withColumn(newNameCol, col(val1) * col(val2))
      mulDataframe
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }

  def divFunction(table: DataFrame, dividendCol: String, divisorCol: String, newNameCol: String): DataFrame = {
    try {
      val divDataframe = table.withColumn(newNameCol, col(dividendCol) / col(divisorCol))
      divDataframe
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }





}
