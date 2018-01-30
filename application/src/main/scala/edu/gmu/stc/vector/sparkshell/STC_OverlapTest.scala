package edu.gmu.stc.vector.sparkshell

import edu.gmu.stc.config.ConfigParameter
import edu.gmu.stc.vector.examples.ShapeFileMetaTest._
import edu.gmu.stc.vector.rdd.{GeometryRDD, ShapeFileMetaRDD}
import edu.gmu.stc.vector.serde.VectorKryoRegistrator
import edu.gmu.stc.vector.sparkshell.STC_BuildIndexTest.logError
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.datasyslab.geospark.enums.IndexType

/**
  * Created by Fei Hu on 1/29/18.
  */
object STC_OverlapTest extends Logging{

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      logError("Please input two arguments: " +
        "\n \t 1) appName: the name of the application" +
        "\n \t 2)configFilePath: this file path for the configuration file path" +
        "\n \t 3) numPartition: the number of partitions")

      return
    }

    val sparkConf = new SparkConf().setAppName(args(0))//.setMaster("local[6]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[VectorKryoRegistrator].getName)


    val sc = new SparkContext(sparkConf)

    val configFilePath = args(1)   //"/Users/feihu/Documents/GitHub/GeoSpark/config/conf.xml"
    val hConf = new Configuration()
    hConf.addResource(new Path(configFilePath))
    sc.hadoopConfiguration.addResource(hConf)

    val tableNames = hConf.get(ConfigParameter.SHAPEFILE_INDEX_TABLES).split(",").map(s => s.toLowerCase().trim)

    val partitionNum = args(2).toInt  //24
    val minX = -180
    val minY = -180
    val maxX = 180
    val maxY = 180

    val shapeFileMetaRDD1 = new ShapeFileMetaRDD(sc, hConf)
    val table1 = tableNames(0)
    shapeFileMetaRDD1.initializeShapeFileMetaRDD(sc, table1, partitionNum, minX, minY, maxX, maxY)
    shapeFileMetaRDD1.indexPartition(IndexType.RTREE)
    shapeFileMetaRDD1.getIndexedShapeFileMetaRDD.cache()
    //println("******shapeFileMetaRDD1****************", shapeFileMetaRDD1.getShapeFileMetaRDD.count())

    val shapeFileMetaRDD2 = new ShapeFileMetaRDD(sc, hConf)
    val table2 = tableNames(1)
    shapeFileMetaRDD2.initializeShapeFileMetaRDD(sc, shapeFileMetaRDD1.getPartitioner, table2,
      partitionNum, minX, minY, maxX, maxY)

    shapeFileMetaRDD2.getShapeFileMetaRDD.cache()

    //println("******shapeFileMetaRDD2****************", shapeFileMetaRDD2.getShapeFileMetaRDD.count())

    println(shapeFileMetaRDD1.getShapeFileMetaRDD.partitions.size, "**********************",
      shapeFileMetaRDD2.getShapeFileMetaRDD.partitions.size)

    val geometryRDD = new GeometryRDD
    geometryRDD.intersect(shapeFileMetaRDD1, shapeFileMetaRDD2, partitionNum)
    logInfo("******** Number of intersected polygons: %d".format(geometryRDD.getGeometryRDD.count()))
  }

}
