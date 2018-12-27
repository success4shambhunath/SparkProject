package com.sham.project

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object HelloSpark  {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("Test App")
        .setMaster("local")
    val sc =new SparkContext(conf)

    val dataArray= Array(1,2,3,3,4)
    val arrRdd= sc.parallelize(dataArray)
    println(arrRdd.partitions.size)

    println( "Hello World!" )
  }

}
