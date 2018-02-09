package edu.gmu.stc.vector.operation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator


/**
  * Created by Fei Hu
  */
object Overlap {

  val isDebug: Boolean = System.getProperty("spark.isDebug", "false").toBoolean

  val LOG: Logger = Logger.getLogger(Overlap.getClass)

  def intersect(sparkSession: SparkSession,
                gridType: String,
                indexType: String,
                shpFile1: String, shpFile2: String,
                numPartitions: Int,
                outputFile: String): Unit = {

    val t = System.currentTimeMillis()

    val plg1RDD = ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, shpFile1)
    val plg2RDD = ShapefileReader.readToPolygonRDD(sparkSession.sparkContext, shpFile2)
    val t1 = System.currentTimeMillis()

    println("************** Read shapefile time: " + (t1 - t)/1000000)

    // get the boundary of RDD
    plg1RDD.analyze()
    plg2RDD.analyze()
    val t2 = System.currentTimeMillis()
    println("************** Analyzing bbox time: " + (t2 - t1)/1000000)


    // partition RDD and buildIndex for each partition
    val PARTITION_TYPE = GridType.getGridType(gridType)
    val INDEX_TYPE = IndexType.getIndexType(indexType)

    plg1RDD.spatialPartitioning(PARTITION_TYPE, numPartitions)
    plg1RDD.buildIndex(INDEX_TYPE, true)
    plg1RDD.indexedRDD = plg1RDD.indexedRDD.cache()


    plg2RDD.spatialPartitioning(plg1RDD.getPartitioner)
    plg2RDD.buildIndex(INDEX_TYPE, true)
    plg2RDD.indexedRDD = plg2RDD.indexedRDD.cache()
    val t3 = System.currentTimeMillis()
    println("************** Partition time: " + (t3 - t2)/1000000)


    // overlap operation
    val intersectRDD = JoinQuery.SpatialIntersectQuery(plg1RDD, plg2RDD, true, true)
    val intersectedResult = new PolygonRDD(intersectRDD)

    println("************** Intersection time: " + (t3 - t2)/1000000)

    println("************** Num polygons: " + intersectedResult.countWithoutDuplicates())

    println("************** Total time: " + (System.currentTimeMillis() - t)/1000000)


    if (isDebug) {
      val numOfPolygons = intersectedResult.countWithoutDuplicates()
      LOG.info(s"****** Number of polygons: $numOfPolygons")
    }

    intersectedResult.saveAsGeoJSON(outputFile)
  }
}
