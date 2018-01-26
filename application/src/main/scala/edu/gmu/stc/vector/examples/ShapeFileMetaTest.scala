package edu.gmu.stc.vector.examples

import edu.gmu.stc.vector.rdd.ShapeFileMetaRDD
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Fei Hu on 1/24/18.
  */
object ShapeFileMetaTest extends App {

  val sparkConf = new SparkConf().setMaster("local[4]").setAppName("ShapeFileMetaTest")
  val sc = new SparkContext(sparkConf)
  val hConf = new Configuration()
  hConf.set("mapred.input.dir", "/Users/feihu/Documents/GitHub/GeoSpark/application/src/main/resources/data/Washington_DC/Impervious_Surface_2015_DC")

  val shapeFileMetaRDD = new ShapeFileMetaRDD(sc, hConf)

  val tableName = "test_123"

  shapeFileMetaRDD.initializeShapeFileMetaRDD()
  shapeFileMetaRDD.saveShapeFileMetaToDB(tableName)

  shapeFileMetaRDD.getShapeFileMetaRDD.foreach( element => {
    println(element.toString)
  })

  val minX = -77.0413824515586
  val minY = 38.9954578167531
  val maxX = -77.0411709654385
  val maxY = 38.9956105073279
  shapeFileMetaRDD.initializeShapeFileMetaList(tableName, minX, minY, maxX, maxY)

  println("***********", shapeFileMetaRDD.getShapeFileMetaList.size)
}
