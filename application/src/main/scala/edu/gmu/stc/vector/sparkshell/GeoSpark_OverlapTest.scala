package edu.gmu.stc.vector.sparkshell

import edu.gmu.stc.vector.operation.Overlap.intersect
import edu.gmu.stc.vector.operation.OperUtil.show_timing
import edu.gmu.stc.vector.sparkshell.OverlapShellExample.sc
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

/**
  * Created by Fei Hu.
  */
object GeoSpark_OverlapTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(args(4))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val sparkSession: SparkSession = sqlContext.sparkSession

    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

    val resourceFolder = args(0) //"/user/root/data0119/"
    val shpFile1: String = resourceFolder + args(1) //"Impervious_Surface_2015_DC"
    val shpFile2: String = resourceFolder + args(2) //"Soil_Type_by_Slope_DC"

    val numPartition: Int = args(3).toInt
    val outputFile: String = resourceFolder + args(4) + ".geojson"

    val runtime: Long = show_timing(intersect(sparkSession, shpFile1, shpFile2, numPartition, outputFile))
    println(s"Runtime is : $runtime seconds")

    sparkSession.stop()
  }
}
