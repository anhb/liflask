package com.anhb.liflask.sparkscala.process

class Run {
  val sparkInit = new SparkInit()
  sparkInit.sparkContextInit()
  sparkInit.sparkSQLInit()
}
