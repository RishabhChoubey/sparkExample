package com.sparkbyexamples.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadTextFiles extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println("##spark read text files from a directory into RDD")
  val rddFromFile = spark.read.option("delimiter",",").csv("src/main/resources/address.csv")
  println(rddFromFile.getClass)
  rddFromFile.printSchema()
  println("##Get data Using collect")
  rddFromFile.collect().foreach(f=>{
    println(f)
  })
  import spark.implicits._
// val df = spark.createDataFrame();
//
//  println("##read multiple text files into a RDD")
//  val rdd4 = spark.sparkContext.textFile("src/main/resources/csv/text01.txt," +
//    "src/main/resources/csv/text02.txt")
//  rdd4.foreach(f=>{
//    println(f)
//  })
//
//  println("##read text files base on wildcard character")
//  val rdd3 = spark.sparkContext.textFile("src/main/resources/csv/text*.txt")
//  rdd3.foreach(f=>{
//    println(f)
//  })
//
//  println("##read all text files from a directory to single RDD")
//  val rdd2 = spark.sparkContext.textFile("src/main/resources/csv/*")
//  rdd2.foreach(f=>{
//    println(f)
//  })
//
//  println("##read whole text files")
//  val rddWhole:RDD[(String,String)] = spark.sparkContext.wholeTextFiles("src/main/resources/csv/text01.txt")
//  println(rddWhole.getClass)
//  rddWhole.foreach(f=>{
//    println(f._1+"=>"+f._2)
//  })
}

