package edu.gmu.stc.vector.application

import java.awt.Color
import java.util

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory, Polygon}
import edu.gmu.stc.vector.transformation.Transformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PolygonRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geosparkviz.core.{ImageGenerator, RasterOverlayOperator}
import org.datasyslab.geosparkviz.extension.visualizationEffect.{HeatMap, ScatterPlot}
import org.datasyslab.geosparkviz.utils.ImageType

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
  * Created by Fei Hu
  */
object Overlap extends App {
  val resourceFolder = System.getProperty("user.dir") + "/application/src/main/resources/data/"
  val shpFile1 = resourceFolder + "la_admin"
  val shpFile2 = resourceFolder + "la_transport"

  val colocationMapLocation = resourceFolder + "la_overlayMap_intersect"

  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("GeoSpark-Analysis")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

  val plg1RDD = ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, shpFile1)
  val plg2RDD = ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, shpFile2)

  // get the boundary of RDD
  plg1RDD.analyze()
  plg2RDD.analyze()

  plg1RDD.spatialPartitioning(GridType.KDBTREE)
  plg1RDD.buildIndex(IndexType.QUADTREE, true)
  plg1RDD.indexedRDD = plg1RDD.indexedRDD.cache()

  plg2RDD.spatialPartitioning(plg1RDD.getPartitioner)
  plg2RDD.buildIndex(IndexType.QUADTREE, true)
  plg2RDD.indexedRDD = plg2RDD.indexedRDD.cache()

  val intersectRDD = JoinQuery.SpatialIntersectQuery(plg1RDD, plg2RDD, true, true)
  val intersectedResult = new PolygonRDD(intersectRDD)
  print("Intersected Polygon Number: " + intersectedResult.countWithoutDuplicates() + "\n")

  intersectedResult.saveAsGeoJSON("la_georesult.json")

  System.out.println("Finished GeoSpark Overlap Spatial Analysis Example")

  sparkSession.stop()
}
