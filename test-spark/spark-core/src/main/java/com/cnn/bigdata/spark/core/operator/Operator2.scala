package com.cnn.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *map方法
 *
 * @author Administrator
 * @date 2021/5/11
 */
object Operator2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4),1)
//   1. rdd的一个分区内的数据是一个一个执行，只有前面的一个数据全部逻辑执行完，才会执行下一个数据
//    分区内数据的执行有序
//    2  分区间数据执行无序
    val rdd1: RDD[Int] = rdd.map((f: Int) => {
      println(">>>>>"+f)
      f
    })

    val rdd2: RDD[Int] = rdd1.map((f: Int) => {
      println("####"+f)
      f
    })

    rdd2.collect()
    sparkContext.stop()
  }
}
