package com.sparkbyexamples.spark.dataframe
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{col, collect_list, to_date, trim}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, _}
import scala.collection.mutable.Map
import java.io.File
import scala.Console.in

object custom   {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val schemaFromCase = StructType(Array(
      StructField("name", StructType(Array(
        StructField("firstname",DoubleType,true),
        StructField("middlename",LongType,true),
        StructField("lastname",DateType,true))),true),
      StructField("namee", TimestampType, true),
      StructField("gender", FloatType, true),
      StructField("salary",IntegerType, true),
        StructField("salary1",ArrayType(IntegerType), true)
    ))




    def printSc(schema:StructType,name:String,m:Map[String,String]):Unit =
    {
      schema.fields.map(f=>{

        val newName= if(name=="") f.name else name+"/"+f.name

        if(m.contains(f.name)) throw new IllegalArgumentException(f.name+" already exist")
     //   println("new name "+ newName)
        f.dataType match{
          case s:StructType=> {


            m+=(newName.split("/").last->f.dataType.typeName)
            printSc(s,newName,m)
            }
          case _=> {

            m += (f.name -> f.dataType.typeName)

          }
        }


      })

    }
val df= spark.createDataFrame(spark.sparkContext.parallelize(List(Row())),schemaFromCase)
  //  println(schemaFromCase("salary1").dataType.typeName.," schema")
    val map:Map[String,String]=Map()
printSc(schemaFromCase,"",map)
    println(map)
 //   println(schemaFromCase("name.firstname").dataType.typeName)
    // import spark.implicits._

//print(spark.version)
//
//    val data = Seq(
//      ( 0,	0,	1000,	2000,	3000 ),
//      ( 1,	1,	1001,	2001,	3001 ),
//      ( 1,	2,	1002,	2002,	3002 ),
//      ( 3,	3,	1003,	2003,	3003 ),
//      ( 4,	4,	1004,	2004,	3004 ),
//      ( 5,	5,	1005,	2005,	3005 )
//
//    )
//
//    val schemaAvro = new Schema.Parser()
//      .parse(new File("src/main/resources/schema.avsc"))
//    val dfavr=spark.read.format("avro").option("avroSchema",schemaAvro.toString()).load
//
//    dfavr.printSchema()
// //    val df=spark.read.format("csv").option("header",true).option("delimiter",",| ,").schema(dfavr.schema).load("src/main/resources/address.csv");
//      val ssc= new StructType().add("a",IntegerType).add("a",IntegerType).add("a",IntegerType).add("a",IntegerType).add("a",IntegerType)
//       val df:DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data))
//    df.show()
//
//     def validatekey(df:DataFrame,key:String,noRecord:Long):Boolean={
//
//       if(df.select(col(key)).distinct().count() != noRecord)
//         return false
//
//     true}
//        def validateKeys(df:DataFrame,keys:String): Boolean ={
//          val keys_List:Array[String]= keys.split(",");
//          val noRecord:Long= df.count()
//          var isValide:Boolean= true
//          for(key <- keys_List)
//            {
//         isValide= validatekey(df,key.trim(),noRecord)
//              if(!isValide)
//                return false
//            }
//
//
//          isValide
//        }
//
//       print( validateKeys(df," _2 ,_3"))
//            spark.createDataFrame(df.rdd,ssc).show()
//       println(df.select("_1").distinct().count)
//        df.groupBy("_1").agg(collect_list("_3"),sql.functions.max("_2"))
//          .toDF("_1","_2","_3").show(); false
//        val bol=  df.filter(col("_4").isNull || trim(col("_4 ")) =="").count()
//        val b=col("_4").isNull
//     if(df.columns.size >0) print("true") else print("false")
//
//
//    //print(d.getClass)
//
//    def colVal(df:DataFrame,noCols:Int): Boolean =
//    {
//      if(df.columns.size<noCols) return false
//
//      true
//    }

    // println(colVal(df,10))


//    Seq(("06/03/2009"),("07-24-2009")).toDF("Date").select(
//      col("Date"),
//      to_date(col("Date"),"MM/dd/yyyy").as("to_date")
//    ).show()

    ////
    //def op(r: Row): Seq[(Int, (Integer, Integer))] = {
    //  val idx = r.apply(0).asInstanceOf[Integer]
    //  val numOtherCols = r.length
    //  (1 until numOtherCols).map { i =>
    //    i -> (idx -> r(i).asInstanceOf[Integer])
    //  }
    //}
    //
    //    def transpose(df: DataFrame): DataFrame = {
    //      val df2 = df.flatMap(op)
    //      println(".................................. after df2")
    //      println()
    //      df2.show()
    //      val df3 = df2.groupBy("_1").agg(collect_list("_2")).toDF("_1", "_2")
    //      println(".................................. after df3")
    //      println()
    //      df3.show();
    //      val df4 = df3.map {
    //        case Row(colIdx: Int, seq: Seq[Row] @unchecked) =>
    //          val data = seq.map { case Row(idx: Integer, x: Integer) => idx -> x } .sortBy(_._1).map(_._2)
    //          colIdx -> data
    //      }
    //      df4.toDF("idx", "values")
    //    }
    //
    //    val df4= transpose(df)
    //    println(".................................. after df4")
    //    println()
    //    df4.show();
    //
    //
    //

  }
}