package com.sparkbyexamples.spark.dataframe


import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable


object FromCSVFile2 {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
import  spark.implicits._
//    val filePath="src/main/resources/csv
    var df3 = spark.read
      df3.option("header",true)
 val df=   df3.csv("src/main/resources/csv/1*.csv")
    df.show(false)


// var m: mutable.Map[String,String]= scala.collection.mutable.Map()
//    m+=("inferSchema"->"true")
//    m+=("delimiter"->"|")
//    m+=("header"->"true")
//    val df = spark.read.options(m).option("quote","\"  ").csv(filePath)
//    df.printSchema()
//    df.show()
//
//    println("map")


//    Seq(("06-03/2009"),("07-24-2009")).toDF("Date").select(
//      col("Date"),
//      to_date(col("Date"),"MM-dd-yyyy").as("to_date")
//    ).show()
//    def validateColumns(row: Row): Row = {
//
//      var err_col: String = null
//      var err_val: String = null
//      var err_desc: String = null
//      val empId = row.getAs[String]("TotalCost")
// //    println(empId)
//
//      // do checking here and populate (err_col,err_val,err_desc) with values if applicable
//
//      row
//
//    }

//    val nn=df.map(row=>{
//     // val util = new Util()
//      val fullName = row.getString(0) +row.getString(1) +row.getString(2)
//      println(fullName)
//      (fullName, row.getString(3),row.getInt(5))
//    })
//    val nd = df.withColumn(df.columns(0), col(df.columns(0)).cast(IntegerType))
//    nd.printSchema()
//    val df2 = df.select("Gender", "BirthDate", "TotalCost", "TotalChildren", "ProductCategoryName")
//      .filter("Gender is not null")
//      .filter("BirthDate is not null")
//      .filter("TotalChildren is not null")
//      .filter("ProductCategoryName is not null")
//    df2.show()
//
//    df.select("Gender", "BirthDate", "TotalCost", "TotalChildren", "ProductCategoryName")
//      .where(df("Gender").isNotNull && df("BirthDate").isNotNull && df("TotalChildren").isNotNull && df("ProductCategoryName").isNotNull ).show()

  }
}
