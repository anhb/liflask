package com.anhb.liflask.sqlutils.functions

import com.anhb.liflask.sqlutils.logs.LoggingVar
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, max, sum}
import org.apache.spark.sql.types.{StructField, StructType}

trait TransformsFunctions extends LoggingVar{

  def sumGroupBy(df: DataFrame, columnGroupBy: String, sumCols: List[String]): DataFrame ={
    try{
      val sum_cols = sumCols.map(colName => sum(colName).as(colName))
      df.groupBy(columnGroupBy).agg(sum_cols.head, sum_cols.tail:_*)
    }catch {
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }

  def maxGroupBy(df: DataFrame, columnGroupBy: String, maxCols: List[String]): DataFrame ={
    try{
      val max_cols = maxCols.map(colName => max(colName).as(colName))
      df.groupBy(columnGroupBy).agg(max_cols.head, max_cols.tail:_*)
    }catch {
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }

  def avgGroupBy(df: DataFrame, columnGroupBy: String, avgCols: List[String]): DataFrame ={
    try{
      val avg_cols = avgCols.map(colName => avg(colName).as(colName))
      df.groupBy(columnGroupBy).agg(avg_cols.head, avg_cols.tail:_*)
    }catch {
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }

  def fillNullCols(tableDF: DataFrame, listCols: Seq[String], fillVal: Int): DataFrame = {
    try {
      tableDF.na.fill(fillVal, listCols)
    }catch {
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }

  def joinTables(df_list: List[DataFrame], columns: Seq[String], joinType: String = "left"): DataFrame = {
    try {
      df_list.reduce(_.join(_, columns, joinType))
    }catch {
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }

  def unionTables(df_list: List[DataFrame]): DataFrame = {
    try {
      df_list.reduce(_.union(_))
    }catch {
      case e: Exception => {
        log_exception
        throw e
      }
    }
  }


  def setNullableStateOfColumn( df: DataFrame, cn: String, nullable: Boolean) : DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField( c, t, _, m) if c.equals(cn) => StructField( c, t, nullable = nullable, m)
      case y: StructField => y
    })
    df.sqlContext.createDataFrame( df.rdd, newSchema)
  }

  def setNullableStateForAllColumns( df: DataFrame, nullable: Boolean) : DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField( c, t, _, m) ⇒ StructField( c, t, nullable = nullable, m)
    })
    df.sqlContext.createDataFrame( df.rdd, newSchema )
  }







}
