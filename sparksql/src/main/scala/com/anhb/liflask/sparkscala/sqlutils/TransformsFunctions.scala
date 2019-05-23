package com.anhb.liflask.sparkscala.sqlutils

import com.anhb.liflask.sparkscala.utils.variablesLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, sum, max, avg}
import org.apache.spark.sql.types.{DecimalType, StructField, StructType}

trait TransformsFunctions extends variablesLog {

  def sumGroupBy(table: DataFrame, columnGroupBy: String, sumCols: List[String]): DataFrame ={
    try{
      val sums = sumCols.map(colName => sum(colName).as(colName))
      val tableDF = table.groupBy(columnGroupBy).agg(sums.head, sums.tail:_*)
      tableDF
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }

  def maxGroupBy(table: DataFrame, columnGroupBy: String, maxCols: List[String]): DataFrame ={
    try{
      val maxs = maxCols.map(colName => max(colName).as(colName))
      val tableDF = table.groupBy(columnGroupBy).agg(maxs.head, maxs.tail:_*)
      tableDF
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }

  def avgGroupBy(table: DataFrame, columnGroupBy: String, avgCols: List[String]): DataFrame ={
    try{
      val avgs = avgCols.map(colName => avg(colName).as(colName))
      val tableDF = table.groupBy(columnGroupBy).agg(avgs.head, avgs.tail:_*)
      tableDF
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }


  def selectCols(tableDF: DataFrame, listCols: List[String]): DataFrame = {
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

  def fillNullCols(tableDF: DataFrame, listCols: List[String], fillVal: Int): DataFrame = {
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

  def joinTables(tables: List[DataFrame], column: String, joinType: String = "left"): DataFrame = {
    try {
      val tableJoin = tables.reduce(_.join(_, Seq(column), joinType))
      tableJoin
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }

  def joinTables(tables: List[DataFrame], columns: Seq[String], joinType: String = "left"): DataFrame = {
    try {
      val tableJoin = tables.reduce(_.join(_, columns, joinType))
      tableJoin
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

  /*Take N number of columns and add withColumn*/
  def castTypes(df: DataFrame): DataFrame = {
    val castDataframe = df
      .withColumn("consolidate_reserve", col("consolidate_reserve").cast(DecimalType(17,2)))
      .withColumn("ead_client", col("ead_client").cast(DecimalType(17,2)))
      .withColumn("lgd_client_mount", col("lgd_client_mount").cast(DecimalType(17,2)))
      .withColumn("pd_amount", col("pd_amount").cast(DecimalType(17,2)))
      .withColumn("lgd_client", col("lgd_client").cast(DecimalType(17,2)))
      .withColumn("econ_mdl_m5_econ_capital_amount", col("econ_mdl_m5_econ_capital_amount").cast(DecimalType(17,2)))
    castDataframe
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
