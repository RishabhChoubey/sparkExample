package com.sparkbyexamples.spark.dataframe.examples

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Stack

import scala.collection.mutable

object RenameDeleteFile extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  //Create Hadoop Configuration from Spark
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  val srcPath=new Path(getClass.getResource("/a.json").getPath)
  val destPath= new Path("/tmp/address_merged.csv")

  //Rename a File
//  if(fs.exists(srcPath) && fs.isFile(srcPath))
//    fs.rename(srcPath,destPath)

  //Alternatively, you can also create Hadoop configuration
  val hadoopConfig = new Configuration()
  val hdfs = FileSystem.get(hadoopConfig)
//println(hdfs.isDirectory(new Path(getClass.getResource("/csv/text 01.txt").getPath)))
  println(hdfs.isFile(new Path("src/main/resources/csv/1*.csv")))
////  if(hdfs.exists(srcPath) && hdfs.isFile(srcPath))
////    hdfs.rename(srcPath,destPath)
//
//  var dirs:mutable.Stack[String] = mutable.Stack()
//  val files = scala.collection.mutable.ListBuffer.empty[String]
//
//  dirs.push(getClass.getResource("/").getPath.toString)
//
//  while(!dirs.isEmpty){
//    val status = hdfs.listStatus(new Path(dirs.pop()))
//    status.foreach(x=> if(x.isDirectory) dirs.push(x.getPath.toString) else
//      files+= x.getPath.toString)
//  }
//  files.foreach(println)
//
//  //Delete a File
////  if(hdfs.isDirectory(srcPath))
////    hdfs.delete(new Path("/tmp/.address_merged2.csv.crc"),true)
//
//  import scala.sys.process._
////  //Delete a File
////  s"hdfs dfs -rm /tmp/.address_merged2.csv.crc" !
////
////  //Delete a Directory
////  s"hdfs dfs -rm -r /tmp/.address_merged2.csv.crc" !
//

}
