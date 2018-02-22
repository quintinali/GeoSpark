package edu.gmu.stc.vector.operation

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by Fei Hu on 1/23/18.
  */
object OperationUtil {

  /**
    * monitor the runtime for the task/function
    *
    * @param proc
    * @tparam T
    * @return
    */
  def show_timing[T](proc: => T): Long = {
    val start=System.nanoTime()
    val res = proc // call the code
    val end = System.nanoTime()
    val runtime = (end-start)/1000000000  //seconds
    runtime
  }

  def getUniqID(id1: Long, id2: Long): Long = {
    (id1 << 32) + id2
  }

  def hdfsToLocal(hdfsPath: String, localPath: String) = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val localFs = FileSystem.getLocal(conf)

    val src = new Path(hdfsPath)

    val dest = new Path(localPath)

    if (localFs.exists(dest)) {
      localFs.delete(dest, true)
    }

    fs.copyToLocalFile(src, dest)
  }

  def main(args: Array[String]): Unit = {
    OperationUtil.hdfsToLocal("/Users/feihu/Documents/GitHub/GeoSpark/config", "/Users/feihu/Desktop/111/111/111/")
  }

}
