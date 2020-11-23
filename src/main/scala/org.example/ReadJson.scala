package org.example

import org.apache.spark.sql.SparkSession

object ReadJson extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.read.json("src/main/resources/zipcodes.json")
  df.printSchema()
  df.show(false)


  spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcode USING json OPTIONS" +
    " (path 'src/main/resources/zipcodes.json')")
  spark.sqlContext.sql("select * fro zipcodes").show(false)

}
