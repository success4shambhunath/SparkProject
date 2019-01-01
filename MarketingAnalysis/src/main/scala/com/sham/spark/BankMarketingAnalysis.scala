package com.sham.spark


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{max, mean, min}
import org.apache.spark.{SparkConf, SparkContext}


object BankMarketingAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(HASHCONFIG.MASTER_URL)
      .setAppName(HASHCONFIG.APPNAME)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    try {
      val csvDF = sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(getClass.getResource("/dataset_bank.csv").getPath)
      println("Bank dataset Schema")
      csvDF.printSchema()
      // marketing success rate. (No. of people subscribed / total no. of entries)
      val numberOfentries = csvDF.count()
      println(s"Number of Entries $numberOfentries")
      val peopleSubscribed = csvDF.filter(csvDF.col("y") === "yes").count()
      println(s"people Subscribed $peopleSubscribed")
      val marketingSuccessRate = peopleSubscribed.toFloat / numberOfentries
      println(f"Marketing Success Rate $marketingSuccessRate")
      val marketingFailureRate = numberOfentries.toFloat / peopleSubscribed
      println(f"Marketing Failure Rate  $marketingFailureRate")
      // Maximum, Mean, and Minimum age of average targeted customer

      val targetedCustomer = csvDF.agg(max("age"), min("age"), mean("age")).head()
      println(targetedCustomer)
      val averageTargetedCustomerMax = targetedCustomer.getInt(0)
      val averageTargetedCustomerMin = targetedCustomer.getInt(1)
      val averageTargetedCustomerAvg = targetedCustomer.getDouble(2)
      println(f"targeted customer \n maximum $averageTargetedCustomerMax \n minimum $averageTargetedCustomerMin \n mean $averageTargetedCustomerAvg")

      println("Top 20 bank records ")
      // csvDF.show()
    } catch {
      case e: Exception => e.printStackTrace()
    }


  }


}
