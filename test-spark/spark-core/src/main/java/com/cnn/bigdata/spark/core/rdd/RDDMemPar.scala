package com.cnn.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 并行度和分区
 *
 * @author Administrator
 * @date 2021/5/9
 */
object RDDMemPar {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)

    //第二个参数是分区数量，不传默认是defaultParallelism（spark.default.parallelism的值）
    //spark.default.parallelism 可以在sparkConf里指定
    //如果spark.default.parallelism没有配置，使用当前环境的最大核数totalCores
//    val rdd: RDD[Int] = sparkContext.makeRDD(List(12, 3, 4, 5, 6), 2)
    val rdd: RDD[Int] = sparkContext.makeRDD(List(12, 3, 4, 5, 6))

    //保存文件
    rdd.saveAsTextFile("output")
    sparkContext.stop()
  }
}
