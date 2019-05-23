package com.anhb.liflask.sparkscala.examples

import com.anhb.liflask.sparkscala.sqlutils.{FSFunctions, IOFunctions, SparkInit, TransformsFunctions}

class Examples extends SparkInit with TransformsFunctions {
  /**
    *  You can use Trait SparkInit to initialize easier SparkContext and SparSession
    *  and Trait TransformsFunctions to get methods for DataFrames
    */
  val sc = sparkContextInit()
  val ss = sparkSQLInit()
  val ioFunctions = new IOFunctions(ss)
  val table = ioFunctions.inputDataframeByMaxDate("/home/bleakmurder/gitProjects/wmuqz_datio_salidasarceapx2/ifrs9/src/test/resources/spark-warehouse/GlobalAssets/Inputs/", "cutoff_date")
  table.show()
  val fsFunctions = new FSFunctions(sc)
  //fsFunctions.renameCSV("", "")
  //val dataframe: DataFrame = sumByClient()


}
