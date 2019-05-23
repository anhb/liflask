package com.anhb.liflask.sparkscala.sqlutils

import com.anhb.liflask.sparkscala.utils.variablesLog
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

class FSFunctions(sparkContext: SparkContext) extends variablesLog{

  def renamePartCSV(path: String, newName: String): Int = {
    try {
      val fs = FileSystem.get(sparkContext.hadoopConfiguration)
      val file = fs.globStatus(new Path(s"${path}/part*.csv"))(0).getPath().getName()
      fs.rename(new Path(s"${path}/" + file),
        new Path(s"${path}/" + s"${newName}".replaceAll("-","" )))
      logger.info("Renamed File")
      0
    }catch {
      case e: Exception => {
        logger.info(excepLog)
        throw e
      }
    }
  }

}
