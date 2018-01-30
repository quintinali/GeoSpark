package edu.gmu.stc.vector.rdd

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import edu.gmu.stc.vector.rdd.index.IndexOperator
import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta
import edu.gmu.stc.vector.shapefile.reader.GeometryReaderUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

/**
  * Created by Fei Hu on 1/26/18.
  */
class GeometryRDD extends Logging{
  private var geometryRDD: RDD[Geometry] = _

  def initialize(shapeFileMetaRDD: ShapeFileMetaRDD, hasAttribute: Boolean):Unit = {
    this.geometryRDD = shapeFileMetaRDD.getShapeFileMetaRDD.mapPartitions(itor => {
      val shapeFileMetaList = itor.toList
      if (hasAttribute) {
        GeometryReaderUtil.readGeometriesWithAttributes(shapeFileMetaList.asJava).asScala.toIterator
      } else {
        GeometryReaderUtil.readGeometries(shapeFileMetaList.asJava).asScala.toIterator
      }
    })
  }

  def intersect(shapeFileMetaRDD1: ShapeFileMetaRDD, shapeFileMetaRDD2: ShapeFileMetaRDD, partitionNum: Int): Unit = {
    val joinRDD: RDD[(ShapeFileMeta, ShapeFileMeta)] = shapeFileMetaRDD1.spatialJoin(shapeFileMetaRDD2, partitionNum)
      .sortBy({case (shapeFileMeta1, shapeFileMeta2) => shapeFileMeta1.getShp_offset})
      .repartition(partitionNum)

    joinRDD.cache()

    logInfo("************** Number of elements in JoinedRDD: %d".format(joinRDD.count()))

    val t1 = System.currentTimeMillis()

    this.geometryRDD = joinRDD.mapPartitions(IndexOperator.spatialIntersect)

    val t2 = System.currentTimeMillis()

    logInfo("******** Intersection takes: %d".format((t2 - t1)/1000))
  }

  def getGeometryRDD: RDD[Geometry] = this.geometryRDD

}
