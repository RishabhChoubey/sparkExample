package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.matching.Regex


object SparkUDF extends App{
  def convertCase(value:String,v2:String) ={
    var error: String=v2

      if(error == null)
        error=" null error "
      else
      {
        error=error +","+ " null"
      }

    error

  }
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  import spark.implicits._
  val columns = Seq("Seqno","Quote")
  val data = Seq(("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "ggfgg")

  )
  val df = data.toDF(columns:_*)
  df.show(false)
var ndf = df.withColumn("error",lit(null))



  val convertL =  (value:String) => {

  val x= new Regex("the")

    x replaceAllIn(value,"THE")

  }

  //Using with DataFrame
  val convertUDF = udf((v1: String, v2:String )=>convertCase(v1,v2))
  val convertUD = udf(convertL)

  // Using it on SQ
  ndf=ndf.withColumn(ndf.columns(2),convertUDF(col(ndf.columns(1)),col("error")))
  ndf.withColumn(ndf.columns(1),convertUD(col("Quote") )).show()
print(col("Quote"))
//
//   spark.udf.register("convertUDF", convertCase)
//  df.createOrReplaceTempView("QUOTE_TABLE")
//
//    spark.sql("select Seqno, convertUDF(Quote,error) from QUOTE_TABLE").show(false)

}
