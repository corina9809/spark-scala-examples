package org.example
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Persist {
  def main(args:Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile("src/main/resources/zipcodes-noheader.csv")

    val rdd2:RDD[ZipCode] = rdd.map(row=>{
      val strArray = row.split(",")
      ZipCode(strArray(0).toInt,strArray(1),strArray(3),strArray(4),strArray(14))
    })
    println("print1")
    rdd2.foreach(println)
    val rdd3 = rdd2.cache()
    println("print2")
    println(rdd3.count())

  }
}
