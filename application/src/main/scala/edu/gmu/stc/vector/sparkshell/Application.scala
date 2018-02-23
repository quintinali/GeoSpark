package edu.gmu.stc.vector.sparkshell

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by Fei Hu on 2/22/18.
  */
object Application extends Logging{

  val sparkConf = new SparkConf().setAppName("Application")

  if (System.getProperty("os.name").equals("Mac OS X")) {
    sparkConf.setMaster("local[6]")
  }

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val sparkSession: SparkSession = sqlContext.sparkSession

  def spatialOperation(args: Array[String], sc: SparkContext, sparkSession: SparkSession): String = {
    if (args.length < 1) {
      logError("Please input the arguments")
    }
    val operationType = args(0)

    operationType match {
      case "GeoSpark_Overlap" => {
        GeoSpark_OverlapTest.overlap(args.slice(1, args.length), sc, sparkSession)
      }

      case "STC_OverlapTest_V1" => {
        STC_OverlapTest_V1.overlap(args.slice(1, args.length), sc, sparkSession)
      }

      case "STC_OverlapTest_V2" => {
        STC_OverlapTest_v2.overlap(args.slice(1, args.length), sc, sparkSession)
      }

      case _ => {
        logError("Please input the right arguments for operations")
        "Please input the right arguments for operations"
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val args_new = Array("STC_OverlapTest_V1", "/Users/feihu/Documents/GitHub/GeoSpark/config/conf_dc.xml", "240", "KDBTREE", "RTREE", "/Users/feihu/Documents/GitHub/GeoSpark/shp_dc_test.shp")
    val output = Application.spatialOperation(args_new, sc, sparkSession)
    println(output)
  }

}
