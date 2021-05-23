package com.epam.spark_hw.scala

import com.epam.spark_hw.scala.flow_controller.TripsFlowControllerScalaImp

object SparkHWScala {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master\\")
    val flowManager = new TripsFlowControllerScalaImp();
    flowManager.handle()
  }

}
