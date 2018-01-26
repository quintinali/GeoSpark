package edu.gmu.stc.vector.examples

import edu.gmu.stc.vector.rdd.ShapeFileMetaRDD
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Fei Hu on 1/24/18.
  */
object ShapeFileMetaTest extends App {

  val sparkConf = new SparkConf().setMaster("local[4]")
    .setAppName("ShapeFileMetaTest")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)

  val sc = new SparkContext(sparkConf)
  val hConf = new Configuration()
  hConf.set("mapred.input.dir", "/Users/feihu/Documents/GitHub/GeoSpark/application/src/main/resources/data/Washington_DC/Impervious_Surface_2015_DC")

  val shapeFileMetaRDD = new ShapeFileMetaRDD

  val tableName = "test_123"

  shapeFileMetaRDD.initializeShapeFileMetaRDD(sc, hConf)
  shapeFileMetaRDD.saveShapeFileMetaToDB(tableName)

  /*shapeFileMetaRDD.getShapeFileMetaRDD.foreach( element => {
    println(element.toString)
  })*/

  val minX = -180
  val minY = -180
  val maxX = 180
  val maxY = 180
  val paritionNum = 10
  shapeFileMetaRDD.initializeShapeFileMetaRDD(sc, tableName, paritionNum, minX, minY, maxX, maxY)

  shapeFileMetaRDD.getShapeFileMetaRDD.mapPartitionsWithIndex((index, itor) => {
    itor.map(shapeFileMeta => (index, shapeFileMeta))
  }, true).foreach(println)
}
