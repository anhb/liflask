package com.anhb.liflask.sparkscala.utils

import com.typesafe.scalalogging.LazyLogging

trait variablesLog extends LazyLogging {
  protected val logAdd: String = "Add new config: ( "
  protected val logAddMiddle: String = " , "
  protected val logAddEnd: String = " )"
  protected val exitCode: Int = 0
  protected val excepLog: String = "Exception Found: "
}
