package com.anhb.liflask.sqlutils.functions

import com.anhb.liflask.sqlutils.logs.LoggingVar
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec

trait SparkFunc extends LoggingVar{

    private val exit_code = 0

    def sparkSQLInit(setNameSpark: String = "spark_master", setMaster: String = "local", listConfig: List[String] = List("spark.ui.enabled"),
                       values: List[String] = List("false"), showOption: Boolean = false): SparkSession = {
        try {
            val sparkSession = SparkSession
              .builder()
              .appName(setNameSpark)
              .config("spark.master", setMaster)
              .getOrCreate()

            @tailrec
            def addConfSession(length: Int, iterator: Int = 0): Int = {
                if (length == iterator) {
                    exit_code
                }
                else {
                    var config: String = listConfig(iterator)
                    var va: String = values(iterator)
                    sparkSession.conf.set(config, va)
                    addConfSession(length, iterator + 1)
                }
            }
            val stop = values.length
            addConfSession(stop)
            if(showOption)sparkSession.conf.getAll.foreach(println)
            sparkSession
        }catch {
            case e: Exception => {
                log_exception
                throw e
            }
        }
    }

    def sparkContextInit(setNameSpark: String = "spark_master", setMaster: String = "local", showOptions: Boolean = false): SparkContext ={
        try {
            val conf = new SparkConf().setAppName(setNameSpark).setMaster(setMaster)
            val sparkContext: SparkContext = new SparkContext(conf)
            if(showOptions) sparkContext.getConf.getAll.foreach(println)
            sparkContext
        }catch {
            case e: Exception => {
                log_exception
                throw e
            }
        }
    }
}
