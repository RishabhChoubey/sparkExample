package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types._
import org.codehaus.commons.compiler.samples.DemoBase.explode

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object FromJsonFile {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
//    val schema = new StructType()
//      .add("dc_id", StringType) // data center
//      .add("source", // info about the source of alarm
//        MapType( // define this as a Map(Key->value)
//          StringType,
//          new StructType()
//            .add("description", StringType)
//            .add("ip", StringType)
//            .add("id", LongType)
//            .add("temp", LongType)
//            .add("c02_level", LongType)
//            .add("geo",
//              new StructType()
//                .add("lat", DoubleType)
//                .add("long", DoubleType)
//            )))
//val r=new Regex("^[a-z]")
//    //read json file into dataframe
//    val df = spark.read.schema(schema).option("multiline", true).json(f"src/main/resources/a.json")
//    df.printSchema()
//    val columns:Array[String]=df.columns;
//    //list for the final column names
//    var col_names:ListBuffer[String]=ListBuffer()
//    //checking data type of each column and adding to the list of final column name
//    for(col<-columns) {
//      // if the col is struct type ..get all fields
//      if (df.schema(col).dataType.isInstanceOf[StructType]) {
//        for (field1 <- df.schema(col).dataType.asInstanceOf[StructType].fields) { // using . to form the column name
//          col_names += col + "." + field1.name
//        }
//      }
//      else {
//        // not a struct col so same col name can be used
//        col_names += col
//      }
//    }
//      for(c <- col_names)
//      print(c+" ")
//
//    df.show(false)

  //   df.select($"dc_id",explode ($"source")).show()


    //read multiline json file
    //    val multiline_df = spark.read.option("multiline", "true")
    //      .json("src/main/resources/multiline-zipcode.json")
    //    multiline_df.printSchema()
    //    multiline_df.show(false)


    //    //read multiple files
        val df2 = spark.read.format("json").option("delimiter",",").option("multiLine",true).load(
          "src/main/resources/multiline-zipcode.json"
        )
        df2.show(false)
    df2.printSchema()
println("false......../////////////")
        //read all files from a folder
        val df3 = spark.read.json("src/main/resources/zipcodes_streaming/*")
        df3.show(false)

        //Define custom schema
        val schema = new StructType()
          .add("City", StringType, true)
          .add("Country", StringType, true)
          .add("Decommisioned", BooleanType, true)
          .add("EstimatedPopulation", LongType, true)
          .add("Lat", DoubleType, false)
          .add("Location", StringType, true)
          .add("LocationText", StringType, true)
          .add("LocationType", StringType, true)
          .add("Long", DoubleType, true)
          .add("Notes", StringType, true)
          .add("RecordNumber", LongType, true)
          .add("State", StringType, true)
          .add("TaxReturnsFiled", LongType, true)
          .add("TotalWages", LongType, true)
          .add("WorldRegion", StringType, true)
          .add("Xaxis", DoubleType, true)
          .add("Yaxis", DoubleType, true)
          .add("Zaxis", DoubleType, true)
          .add("Zipcode", StringType, true)
          .add("ZipCodeType", StringType, true)

        val df_with_schema = spark.read.schema(schema).json("src/main/resources/zipcodes.json")
    println("false,,,,,,,,,,,,,,,,")
        df_with_schema.printSchema()
        df_with_schema.show(false)

        spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcode USING json OPTIONS (path 'src/main/resources/zipcodes.json')")
        spark.sqlContext.sql("SELECT * FROM zipcode").show()

        //Write json file

        df2.write.mode(SaveMode.Overwrite)
          .json("/tmp/spark_output/zipcodes1.json")
  }
}