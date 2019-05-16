package com.anhb.liflask.sparkscala.process

import org.apache.spark.{SparkConf, SparkContext}
import scala.annotation.tailrec
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.LazyLogging

trait SparkInit extends LazyLogging {

    protected val logAdd: String = "Add new config: ( "
    protected val logAddMiddle: String = " , "
    protected val logAddEnd: String = " )"
    protected val exitCode: Int = 0
    protected val excepLog: String = "Exception Found: "

    def sparkSQLInit(setNameSpark: String = "spark_master", setMaster: String = "local", listConfig: List[String] = List("spark.ui.enabled"),
                       values: List[String] = List("false"), showOption: Boolean = false): SparkSession = {
        try {
            val sparkSession = SparkSession
              .builder()
              .appName(setNameSpark)
              .config("spark.master", setMaster)
              .getOrCreate()

            import sparkSession.implicits._

            @tailrec
            def addConfSession(length: Int, iterator: Int = 0): Int = {
                if (length == iterator) {
                    exitCode
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
                logger.error(excepLog)
                throw e
            }
        }
    }

    def sparkContextInit(setNameSpark: String = "spark_master", setMaster: String = "local", listConfig: List[String] = List("spark.ui.enabled"),
                         values: List[String] = List("true"), showOption: Boolean = true): SparkContext ={
        try {
            var conf = new SparkConf().setAppName(setNameSpark).setMaster(setMaster)
            val sparkContext: SparkContext = new SparkContext(conf)
            if(showOption) sparkContext.getConf.getAll.foreach(println)
            sparkContext
        }catch {
            case e: Exception => {
                logger.error(excepLog)
                throw e
            }
        }
    }
}
