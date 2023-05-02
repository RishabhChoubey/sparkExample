package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import java.io.File

object StringToTimestamp extends App {
   val warehouseLocation=new File("spark-warehouse").getAbsolutePath
  System.setProperty("hadoop.home.dir","C:/hadoop")
  println(warehouseLocation)
  val spark:SparkSession = SparkSession.builder()
    .master("local")
  //  .config("spark.sql.warehouse.dir",warehouseLocation)
    .appName("SparkByExamples.com")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


 // spark.sql("create table if not exits src(key INT,value String) using hive ");
  import spark.sqlContext.implicits._

//  //String to timestamps
//  val df = Seq(("2019-07-01 12:01:19.000"),
//    ("2019-06-24 12:01:19.000"),
//    ("2019-11-16 16:44:55.406"),
//    ("2019-11-16 16:50:59.406")).toDF("input_timestamp")
//df.write.format("csv").save("sample")
//  df.withColumn("datetype_timestamp",
//        to_timestamp(col("input_timestamp")))
//    .printSchema()
//
//
//  //Convert string to timestamp when input string has just time
  val df1 = Seq(
  ("12:01:19.345"),
    ("12:01:20.567"),
    ("16:02:44.045"),
    ("16:50:59.406"))
    .toDF()

//  df1.withColumn("datetype_timestamp",
//    to_timestamp(col("input_timestamp"),"HH:mm:ss.SSS"))
//    .show(false)
//
//  //when dates are not in Spark DateType format 'yyyy-MM-dd  HH:mm:ss.SSS'.
//  //Note that when dates are not in Spark DateType format, all Spark functions returns null
//  //Hence, first convert the input dates to Spark DateType using to_timestamp function
//
//  def fm():Boolean= {
//      val dfDate = Seq(("07-01-2019 12"),
//        ("06-24-2019 12"),
//        ("11-16-2019 16"),
//        ("11-16-2019 16")).toDF("input_timestamp")
//
//      dfDate.withColumn("datetype_timestamp",
//        to_timestamp(trim(col("input_timestamp")), "MM-dd-yyyy ".trim))
//        .show(false)
//      true
//
//  }
//  println(fm())
}
