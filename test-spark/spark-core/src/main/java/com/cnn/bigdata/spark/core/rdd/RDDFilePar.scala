package com.cnn.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 文件数据分区规则
 *
 * @author Administrator
 * @date 2021/5/9
 */
object RDDFilePar {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    //第二个参数：minPartitions-最小分区，默认值defaultParallelism，未配置则使用默认值2
    //分区可能比minPartitions大，本质是使用的hadoop的分区规则:
    // 每个分区大小：goalSize=总文件字节数（7）/minPartitions（2）=3
    // 分区数：7/3=2..1, 如果多出的部分超10%，会新增分区，这里多出的部分达30%了，会新生成一个分区）

    //1. spark这里的读取文件使用的是hadoop的文件读取，且是按行读取的，和字节数没有关系。
    //2. 数据读取时以偏移量为单位,偏移量不会被重复读取
    // 如文件内容(@@代表回车换行)：
//    1@@  -->偏移量为：012
//    2@@  -->偏移量为：345
//    3    -->偏移量为：6
//    3.数据分区的偏移量范围计算
//    0分区：-》[0,3]  读取到12@@，因为时按行读取，偏移量45也会被读取到
//    1分区：-》[3,6]  从偏移量3开始，但是已被读取过，所以从6开始，读到3
//    2分区：-》[6,7]  从偏移量6开始，但是已被读取过，从7开始，读到空

    val fileRdd: RDD[String] = sparkContext.textFile("data/3.txt")

    //保存文件
    fileRdd.saveAsTextFile("output")
    sparkContext.stop()
  }
}
