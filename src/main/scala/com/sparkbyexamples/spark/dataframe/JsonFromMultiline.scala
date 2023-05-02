package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object   JsonFromMultiline extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  val schema= new StructType().add("City",StringType).add("RecordNumber",LongType).add("State",StringType)
    .add("ZipCodeType",StringType).add("Zipcode",IntegerType)

  //read multiline json file
  val multiline_df = spark.read.option("multiline", "true").schema(schema )
    .json("src/main/resources/multiline-zipcode.json")
  multiline_df.printSchema()
  multiline_df.show(false)
  val re=(s:String,p:String)=>
  {
    s.matches(p)
  }
  val mat= udf(re)
 println( multiline_df.withColumn("regex",mat(col("Zipcode"),lit("[0-9]{3}"))).filter("State like '_R' and RecordNumber == 2").count())
println("..........................................................................")
}
