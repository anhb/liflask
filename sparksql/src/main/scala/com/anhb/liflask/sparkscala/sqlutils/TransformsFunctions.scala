package com.anhb.liflask.sparkscala.sqlutils

import com.anhb.liflask.sparkscala.process.SparkInit
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum

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

  def joinMul(tables: List[DataFrame], cols: Seq[String]): DataFrame = {
    try {
      val tableJoin = tables.reduce(_.join(_, cols, "left"))
      tableJoin
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }



}
