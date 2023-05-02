package com.sparkbyexamples.spark.dataframe.functions.aggregate

import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import scala.collection.mutable.ListBuffer
object DistinctCount extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  val df = simpleData.toDF("employee_name", "department", "salary")
  df.printSchema()

  val structureData = Seq(
    Row(Row("James","","Smith"),"36636","M",3100),
    Row(Row("Michael","Rose",""),"40288","M",4300),
    Row(Row("Robert","","Williams"),"42114","M",1400),
    Row(Row("Maria","Anne","Jones"),"39192","F",5500),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
  )

  val structureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("id",StringType)
    .add("gender",StringType)
    .add("salary",IntegerType)

  val dff = spark.createDataFrame(
    spark.sparkContext.parallelize(structureData),structureSchema)
  def getColumns(sch:StructType, pref:String= null):ListBuffer[Column]=
    {
      var a:ListBuffer[Column]=ListBuffer()

      sch.fields.map(f=>{
         val colName= if(pref =="") f.name else pref+"."+f.name

   var temp:ListBuffer[Column]=  f.dataType match {
    case s:StructType => getColumns(s,colName)
    case _ => ListBuffer(col(colName).as(colName))


}
     a= a ++ temp

      })
a
    }
    val c= getColumns(dff.schema,"")
  //c.toIterator
  c.foreach(
   println
  )
  dff.select(c:_*).show()

  println("Distinct Count: " + df.distinct().count())

  val df2 = df.select(countDistinct("department", "salary"))
  df2.show(false)
  df.select("employee_name").distinct().show()

  println("Distinct Count of Department & Salary: "+df2.collect()(0)(0))

}