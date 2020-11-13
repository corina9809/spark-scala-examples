package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Basic {
  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    val data = Array(1, 2, 3, 4, 5)
    val distData = spark.sparkContext.parallelize(data)

  }

}
