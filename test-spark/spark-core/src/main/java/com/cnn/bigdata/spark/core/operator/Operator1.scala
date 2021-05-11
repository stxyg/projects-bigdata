package com.cnn.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *map方法
 * @author Administrator
 * @date 2021/5/11
 */
object Operator1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4))
//    val rdd1: RDD[Int] = rdd.map((f: Int) => {f*2})
//    val rdd1: RDD[Int] = rdd.map((f: Int) => f*2)
//    val rdd1: RDD[Int] = rdd.map(f => f*2)
//    参数只出现一次，且按顺序，可用_代替
    val rdd1: RDD[Int] = rdd.map(_*2)


    rdd1.collect().foreach(println)
    sparkContext.stop()
  }
}
