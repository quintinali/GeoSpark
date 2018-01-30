package edu.gmu.stc.vector.rdd

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.index.SpatialIndex
import edu.gmu.stc.hibernate.{DAOImpl, HibernateUtil, PhysicalNameStrategyImpl}
import edu.gmu.stc.vector.operation.OperUtil
import edu.gmu.stc.vector.parition.{PartitionUtil, SpatialPartitioner}
import edu.gmu.stc.vector.rdd.index.IndexOperator
import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta
import edu.gmu.stc.vector.shapefile.meta.index.ShapeFileMetaIndexInputFormat
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey
import org.hibernate.Session

import scala.collection.JavaConverters._



/**
  * Created by Fei Hu on 1/24/18.
  */

class ShapeFileMetaRDD (sc: SparkContext, @transient conf: Configuration) extends Serializable with Logging {
  private var shapeFileMetaRDD: RDD[ShapeFileMeta] = _

  private var indexedShapeFileMetaRDD: RDD[SpatialIndex] = _

  private var partitioner: SpatialPartitioner = _

  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))

  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    conf
  }

  def initializeShapeFileMetaRDD(sc: SparkContext, conf: Configuration): Unit = {
    shapeFileMetaRDD = new NewHadoopRDD[ShapeKey, ShapeFileMeta](sc,
      classOf[ShapeFileMetaIndexInputFormat].asInstanceOf[Class[F] forSome {type F <: InputFormat[ShapeKey, ShapeFileMeta]}],
      classOf[org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey],
      classOf[edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta],
      conf).map( element => element._2)
  }

  def initializeShapeFileMetaRDD(sc: SparkContext,
                                 tableName: String,
                                 partitionNum: Int, minX: Double, minY: Double,
                                 maxX: Double, maxY: Double): Unit = {
    val physicalNameStrategy = new PhysicalNameStrategyImpl(tableName)
    val session = HibernateUtil
      .createSessionFactoryWithPhysicalNamingStrategy(sc.hadoopConfiguration, physicalNameStrategy,
        classOf[ShapeFileMeta])
      .openSession
    val dao = new DAOImpl[ShapeFileMeta]()
    dao.setSession(session)
    val hql = ShapeFileMeta.getSQLForOverlappedRows(tableName, minX, minY, maxX, maxY)

    val shapeFileMetaList = dao.findByQuery(hql, classOf[ShapeFileMeta]).asScala.toList

    logInfo("Number of queried shapefile metas is : " + shapeFileMetaList.size)

    session.close()

    partitioner = PartitionUtil.spatialPartitioning(GridType.RTREE, partitionNum, shapeFileMetaList.asJava)

    shapeFileMetaRDD = sc.parallelize(shapeFileMetaList, partitionNum)
      .flatMap(shapefileMeta => partitioner.placeObject(shapefileMeta).asScala)
      .partitionBy(partitioner)
      .map(tuple => tuple._2)
      /*.map( tuple => (tuple._2.getIndex, tuple._2))
      .reduceByKey((shapeFileMeta1, shapeFileMeta2) => shapeFileMeta1)
      .map(tuple => tuple._2)*/
  }

  def initializeShapeFileMetaRDD(sc: SparkContext, partitioner: SpatialPartitioner,
                                 tableName: String, partitionNum: Int,
                                 minX: Double, minY: Double, maxX: Double, maxY: Double) = {
    val physicalNameStrategy = new PhysicalNameStrategyImpl(tableName)
    val session = HibernateUtil
      .createSessionFactoryWithPhysicalNamingStrategy(sc.hadoopConfiguration, physicalNameStrategy,
        classOf[ShapeFileMeta])
      .openSession
    val dao = new DAOImpl[ShapeFileMeta]()
    dao.setSession(session)
    val hql = ShapeFileMeta.getSQLForOverlappedRows(tableName, minX, minY, maxX, maxY)

    val shapeFileMetaList = dao.findByQuery(hql, classOf[ShapeFileMeta]).asScala.toList
    session.close()

    this.partitioner = partitioner

    shapeFileMetaRDD = sc.parallelize(shapeFileMetaList, partitionNum)
      .flatMap(shapefileMeta => this.partitioner.placeObject(shapefileMeta).asScala)
      .partitionBy(this.partitioner)
      .map(tuple => tuple._2)
      /*.map( tuple => (tuple._2.getIndex, tuple._2))
      .reduceByKey((shapeFileMeta1, shapeFileMeta2) => shapeFileMeta1)
      .map(tuple => tuple._2)*/
  }

  def saveShapeFileMetaToDB(conf: Configuration, tableName: String): Unit = {
    shapeFileMetaRDD.foreachPartition(itor => {
      val physicalNameStrategy = new PhysicalNameStrategyImpl(tableName)
      val session = HibernateUtil
        .createSessionFactoryWithPhysicalNamingStrategy(conf, physicalNameStrategy,
                                                        classOf[ShapeFileMeta])
        .openSession
      val dao = new DAOImpl[ShapeFileMeta]()
      dao.setSession(session)
      dao.insertDynamicTableObjectList(tableName, itor.asJava)
      session.close()
    })
  }

  def saveShapeFileMetaToDB(): Unit = {
    shapeFileMetaRDD.foreachPartition(itor => {
      val shapeFileMetaList = itor.toList
      //TODO: make sure the table name is right
      val tableName = shapeFileMetaList.head.getFilePath.split("/").last.toLowerCase

      logInfo("******* Save into the table [%s]".format(tableName))

      val physicalNameStrategy = new PhysicalNameStrategyImpl(tableName)
      val session = HibernateUtil
        .createSessionFactoryWithPhysicalNamingStrategy(getConf, physicalNameStrategy,
          classOf[ShapeFileMeta])
        .openSession
      val dao = new DAOImpl[ShapeFileMeta]()
      dao.setSession(session)
      dao.insertDynamicTableObjectList(tableName, shapeFileMetaList.asJava.iterator())
      session.close()
    })
  }

  def partition(partitioner: SpatialPartitioner): Unit = {
    this.shapeFileMetaRDD = this.shapeFileMetaRDD
      .flatMap(shapefileMeta => this.partitioner.placeObject(shapefileMeta).asScala)
      .partitionBy(this.partitioner).map( tuple => tuple._2).distinct()
  }

  def indexPartition(indexType: IndexType) = {
    val indexBuilder = new IndexOperator(indexType.toString)
    this.indexedShapeFileMetaRDD = this.shapeFileMetaRDD.mapPartitions(indexBuilder.buildIndex)
  }

  def spatialJoin(shapeFileMetaRDD2: ShapeFileMetaRDD, partitionNum: Int): RDD[(ShapeFileMeta, ShapeFileMeta)] = {
    this.indexedShapeFileMetaRDD
      .zipPartitions(shapeFileMetaRDD2.getShapeFileMetaRDD)(IndexOperator.spatialJoin)
      .map(tuple => (OperUtil.getUniqID(tuple._1.getIndex, tuple._2.getIndex), tuple))
      .reduceByKey((tuple1:(ShapeFileMeta, ShapeFileMeta), tuple2: (ShapeFileMeta, ShapeFileMeta)) => tuple1)
      //.sortByKey(ascending = true, partitionNum)
      .map(tuple => tuple._2)
  }

  def spatialIntersect(shapeFileMetaRDD2: ShapeFileMetaRDD): RDD[Geometry] = {
    this.indexedShapeFileMetaRDD
      .zipPartitions(shapeFileMetaRDD2.getShapeFileMetaRDD, preservesPartitioning = true)(IndexOperator.spatialIntersect)
  }

  def getShapeFileMetaRDD: RDD[ShapeFileMeta] = this.shapeFileMetaRDD

  def getPartitioner: SpatialPartitioner = this.partitioner

  def getIndexedShapeFileMetaRDD: RDD[SpatialIndex] = this.indexedShapeFileMetaRDD
}