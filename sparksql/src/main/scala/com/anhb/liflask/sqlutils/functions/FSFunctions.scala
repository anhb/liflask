package com.anhb.liflask.sqlutils.functions

import com.anhb.liflask.sqlutils.logs.LoggingVar
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

class FSFunctions(sparkContext: SparkContext) extends LoggingVar{

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
        log_exception
        throw e
      }
    }
  }

}
