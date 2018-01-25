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
/*class ShapeFileMetaRDD (sc: SparkContext, @transient conf: Configuration)
  extends NewHadoopRDD(sc,
    classOf[ShapeFileMetaIndexInputFormat].asInstanceOf[Class[F] forSome {type F <: InputFormat[ShapeKey, ShapeFileMeta]}],
    classOf[org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey],
    classOf[edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta],
    conf){

}*/


class ShapeFileMetaRDD (sc: SparkContext, conf: Configuration) {

  val shapeFileMetaRDD: RDD[ShapeFileMeta] = new NewHadoopRDD[ShapeKey, ShapeFileMeta](sc,
    classOf[ShapeFileMetaIndexInputFormat].asInstanceOf[Class[F] forSome {type F <: InputFormat[ShapeKey, ShapeFileMeta]}],
    classOf[org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey],
    classOf[edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta],
    conf).map( element => element._2)

  def toDatabase(tableName: String): Unit = {
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
}