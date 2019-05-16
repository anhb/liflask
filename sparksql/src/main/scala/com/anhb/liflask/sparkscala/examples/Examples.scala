package com.anhb.liflask.sparkscala.examples

import com.anhb.liflask.sparkscala.process.SparkInit

class Examples extends SparkInit {
  /**
    *  You can use Trait SparkInit to initialize easier SparkContext and SparSession.
    *  Get methods to specific parameters like parameters and configurations
    */
  sparkContextInit()
  sparkSQLInit()
}
