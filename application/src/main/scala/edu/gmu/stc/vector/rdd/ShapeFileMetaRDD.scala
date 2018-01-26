package edu.gmu.stc.vector.rdd

import edu.gmu.stc.hibernate.{DAOImpl, HibernateUtil, PhysicalNameStrategyImpl}
import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta
import edu.gmu.stc.vector.shapefile.meta.index.ShapeFileMetaIndexInputFormat
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.internal.Logging
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey
import org.hibernate.Session

import scala.collection.JavaConverters._


/**
  * Created by Fei Hu on 1/24/18.
  */

class ShapeFileMetaRDD (sc: SparkContext, conf: Configuration) {
  private var shapeFileMetaList: List[ShapeFileMeta] = _

  private var shapeFileMetaRDD: RDD[ShapeFileMeta] = _

  def initializeShapeFileMetaRDD(): Unit = {
    shapeFileMetaRDD = new NewHadoopRDD[ShapeKey, ShapeFileMeta](sc,
      classOf[ShapeFileMetaIndexInputFormat].asInstanceOf[Class[F] forSome {type F <: InputFormat[ShapeKey, ShapeFileMeta]}],
      classOf[org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey],
      classOf[edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta],
      conf).map( element => element._2)
  }

  def initializeShapeFileMetaList(tableName: String, minX: Double, minY: Double,
                                  maxX: Double, maxY: Double): Unit = {
    val physicalNameStrategy = new PhysicalNameStrategyImpl(tableName)
    val session = HibernateUtil
      .createSessionFactoryWithPhysicalNamingStrategy(physicalNameStrategy,
        classOf[ShapeFileMeta])
      .openSession
    val dao = new DAOImpl[ShapeFileMeta]()
    dao.setSession(session)
    val hql = ShapeFileMeta.getSQLForOverlappedRows(tableName, minX, minY, maxX, maxY)
    shapeFileMetaList = dao.findByQuery(hql, classOf[ShapeFileMeta]).asScala.toList
    session.close()
  }

  def saveShapeFileMetaToDB(tableName: String): Unit = {
    shapeFileMetaRDD.foreachPartition(itor => {
      val physicalNameStrategy = new PhysicalNameStrategyImpl(tableName)
      val session = HibernateUtil
        .createSessionFactoryWithPhysicalNamingStrategy(physicalNameStrategy,
                                                        classOf[ShapeFileMeta])
        .openSession
      val dao = new DAOImpl[ShapeFileMeta]()
      dao.setSession(session)
      dao.insertDynamicTableObjectList(tableName, itor.asJava)
      session.close()
    })
  }

  def getShapeFileMetaList: List[ShapeFileMeta] = this.shapeFileMetaList

  def getShapeFileMetaRDD: RDD[ShapeFileMeta] = this.shapeFileMetaRDD
}