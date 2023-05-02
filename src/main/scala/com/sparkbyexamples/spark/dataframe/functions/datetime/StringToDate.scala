package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object StringToDate extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  Seq(("1-01-2009 12"),("2009")).toDF("Date").select(
    col("Date"),
    to_date(col("Date"),"M-d-yyyy H").as("to_date")
  ).show()
}
