package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.conf.Configuration
import java.io.File
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
object JsonToAvroCsvParquet extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  def getFileName(): Column = {
    val file_name = reverse(split(input_file_name(), "/")).getItem(0)
    val f=input_file_name()
    println(file_name)
    val s=split(file_name, "_").getItem(0)
    println(s)
s
  }
  //read json file into dataframe
  val df = spark.read.json("src/main/resources/zipcodes.json")
  df.printSchema()
 df.show(false)
  val df1 = spark.read.option("recursiveFileLookup",true).option("header",true).csv("/tmp/csv/zipcodescsv/76177/STANDARD/")

  df1.withColumn("name_col",getFileName()).show()
//
//  //convert to avro
//  df.write.format("avro").mode(SaveMode.Overwrite).save("/tmp/avro/zipcodes.avro")
//
//  //convert to avro by partition
//  df.write.partitionBy("State","Zipcode")
//    .format("avro").mode(SaveMode.Overwrite).save("/tmp/avro/zipcodes_partition.avro")
//
//  //convert to parquet
//  df.write.mode(SaveMode.Overwrite).parquet("c:/tmp/parquet/zipcodes.parquet")
// df.write.mode(SaveMode.Append).json("/tmp/json/risbh.json")
//val jspndf= spark.read.json("/tmp/json/risbh.json")
//  println(" ................... json df .....................")
//  jspndf.show()
//  //convert to csv
  //df.write.option("header","true").partitionBy("Zipcode","ZipCodeType") .mode(SaveMode.Overwrite).csv("/tmp/csv/zipcodescsv")
//  val hadoopCongif= new Configuration()
//  val hdfs= FileSystem.get(hadoopCongif)
//  val path="/tmp/csv/zipcodes.csv"
// // val fList= FileUtil.listFiles(new File("/tmp/csv/zipcodescsv"))
//
//
//
//  def changeNameOfDir(fileName:String,fileNewName:String,format:String):Unit= {
//    println(fileName, " filename  check")
//    if ((!fileName.contains("SUC") && !fileName.endsWith(".crc")) && fileName.endsWith("." + format)) {
//      println(fileName, " file .csv")
//      val nameBreak = fileName.split('\\')
//      var newName: String = ""
//      for (i <- 0 until (nameBreak.length - 1))
//        newName += nameBreak(i) + "/"
//
//      FileUtil.copy(hdfs, new Path(fileName), hdfs, new Path(newName + fileNewName + "." + format), true, hadoopCongif)
//      hdfs.delete(new Path(fileName), true)
//    }
//    else if ((!fileName.contains("SUC") && !fileName.endsWith(".crc"))) {
//      println(fileName, " file dir")
//
//      val fList = FileUtil.listFiles(new File(fileName))
//      fList.foreach(file => {
//        changeNameOfDir(file.toString, fileNewName, format)
//
//
//      })
//      val len = fileName.toString.split('\\')
//      // len.foreach(println)
//
//      var newPath = ""
//      for (i <- 0 until (len.length - 1)) {
//        newPath += len(i) + "/"
//      }
//
//      newPath += len(len.length - 1).split("=").last
//      println(newPath)
//      if(hdfs.exists(new Path(fileName.toString))) {
//        hdfs.rename(new Path(fileName.toString), new Path(newPath))
//       // hdfs.deleteOnExit(new Path(fileName.toString))
//      }
//
//    }
//  }
//
//  changeNameOfDir("/tmp/csv/zipcodescsv","newFile","csv")
//
//
//
//
//
//
//
//
//
////
////  fList.foreach(f=> {
////
////
////    val len = f.toString.split('\\')
////   // len.foreach(println)
////    if (len(len.length - 1).startsWith("Zip")) {
////      var newPath = ""
////      for (i <- 0 until (len.length - 1)) {
////        newPath += len(i) + "/"
////      }
////
////      newPath += len(len.length - 1).split("=")(1)
////      println(newPath)
////      if(hdfs.exists(new Path(f.toString))) {
////        hdfs.rename(new Path(f.toString), new Path(newPath))
////      hdfs.deleteOnExit(new Path(f.toString))
////      }
////    }
////  })
////}

}
