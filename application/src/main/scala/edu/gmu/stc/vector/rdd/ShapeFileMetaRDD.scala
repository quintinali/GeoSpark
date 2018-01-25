package edu.gmu.stc.vector.rdd

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta
import edu.gmu.stc.vector.shapefile.meta.index.ShapeFileMetaIndexInputFormat
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.mapreduce.InputFormat
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey

/**
  * Created by Fei Hu on 1/24/18.
  */
class ShapeFileMetaRDD (sc: SparkContext, @transient conf: Configuration)
  extends NewHadoopRDD(sc,
    classOf[ShapeFileMetaIndexInputFormat].asInstanceOf[Class[F] forSome {type F <: InputFormat[ShapeKey, ShapeFileMeta]}],
    classOf[org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey],
    classOf[edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta],
    conf){

}