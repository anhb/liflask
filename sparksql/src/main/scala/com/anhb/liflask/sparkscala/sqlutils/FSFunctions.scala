package com.anhb.liflask.sparkscala.sqlutils

import com.anhb.liflask.sparkscala.process.SparkInit
import org.apache.hadoop.fs.{FileSystem, Path}

object FSFunctions extends SparkInit{

  def renameCSV(path: String, newName: String): Int = {
    val sparkContext = sparkContextInit()
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
