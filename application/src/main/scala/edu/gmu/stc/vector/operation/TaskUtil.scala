package edu.gmu.stc.vector.operation

/**
  * Created by Fei Hu on 1/23/18.
  */
object TaskUtil {

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

}
