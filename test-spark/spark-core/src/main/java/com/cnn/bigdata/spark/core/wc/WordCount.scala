package com.cnn.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author Administrator
 * @date 2021/5/6
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //本地模式
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)
    //读取文件行
    val lines: RDD[String] = sc.textFile("datas")
    //分词-扁平话
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //根据单词分组统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy((word: String) => word)
    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //将结果收集打印到控制台
    val arr: Array[(String, Int)] = wordToCount.collect()
    arr.foreach(println)
    //关闭连接
    sc.stop()

  }

}
