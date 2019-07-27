package com.spark

import org.apache.spark.{SparkConf, SparkContext}

object FirstSparkApplication {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf()
    conf.setAppName("FirstApplication")
    conf.setMaster("local")
    val sc =new SparkContext(conf)
    val input = sc.textFile("D:/Karthik/Common.txt")

    val count1 = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)
    count1.saveAsTextFile("D:/Karthik/wordcount/")
    //count.saveAsTextFile("/users/sairavi/wordcount/")
    System.out.println("OK")
  }

}
