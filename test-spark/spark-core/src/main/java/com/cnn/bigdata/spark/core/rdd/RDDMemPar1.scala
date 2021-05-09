package com.cnn.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 内存数据分区规则
 *
 *
 * @author Administrator
 * @date 2021/5/9
 */
object RDDMemPar1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
  //可以看源码得出分区规则
//  def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
//    (0 until numSlices).iterator.map { i =>
//      val start = ((i * length) / numSlices).toInt
//      val end = (((i + 1) * length) / numSlices).toInt
//      (start, end)
//    }
//  }
    val rdd: RDD[Int] = sparkContext.makeRDD(List(12, 3, 4, 5, 6),2)

    //保存文件
    rdd.saveAsTextFile("output")
    sparkContext.stop()
  }
}
