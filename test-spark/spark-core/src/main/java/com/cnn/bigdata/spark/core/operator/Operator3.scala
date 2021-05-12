package com.cnn.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author Administrator
 * @date 2021/5/11
 */
object Operator3 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4),3)
//  一个分区的数据全部拿到再操作，效率比map高
    //整个分区的数据放在内存，不会被释放，大数据量容易内存溢出。map不会有这个问题
  val mpRDD: RDD[Int] = rdd.mapPartitions(itr => {
    println(">>>>>>")
    itr.map(_ * 2)
  })

    mpRDD.collect().foreach(println)
    sparkContext.stop()
  }
}
