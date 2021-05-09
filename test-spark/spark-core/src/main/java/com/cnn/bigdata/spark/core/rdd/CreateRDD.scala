package com.cnn.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 创建RDD：4种
 *
 * @author Administrator
 * @date 2021/5/9
 */
object CreateRDD {
  def main(args: Array[String]): Unit = {
    //1.从集合（内存创建）rdd:parallelize和makeRDD
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val rdd1: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7))
    //本质还是调用parallelize方法
    val rdd2: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 7, 4, 58, 9, 6, 7))
    rdd1.collect().foreach(print)
    rdd2.collect().foreach(print)

    //2.从外部存储（文件）创建RDD,如：本地文件，HDFS，HBase.
    //path可以是相对路径，绝对路径；
    //path可以是文件完整路径，也可以是目录
    //path支持通配符，如data/1*.txt
    val fileRdd: RDD[String] = sparkContext.textFile("data/1*.txt")
    fileRdd.collect().foreach(println)

    //wholeTextFiles 是以文件为单位读取，返回2个元素：文件路径，文件内容，textFile以行为单位读取
    val filesRdd: RDD[(String, String)] = sparkContext.wholeTextFiles("data")
    filesRdd.collect().foreach(print)

    //3. 从其他RDD创建：通过一个RDD运算完成后，再产生新的RDD，一般spark框架使用
    //4. 直接创建（new）：一般spark框架自身使用


    sparkContext.stop();

  }
}
