package com.sparkbyexamples.spark.rdd
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, concat, explode, floor, lit, rand,split,col}


object CreateRDD {

  def main(args:Array[String]): Unit ={

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExamples.com")
      .getOrCreate()

//
//    //Create DataFrame df1 with columns name,dept & age
//    val data = Seq(("James","Sales",34), ("Michael","Sales",56),
//      ("Robert","Sales",30), ("Maria","Finance",24) )
//    import spark.implicits._
//    val df1 = data.toDF("name","dept","age")
//    df1.printSchema()
//
//    //root
//    // |-- name: string (nullable = true)
//    // |-- dept: string (nullable = true)
//    // |-- age: long (nullable = true)
//
//    //Create DataFrame df1 with columns name,dep,state & salary
//    val data2=Seq(("James","Sales","NY",9000),("Maria","Finance","CA",9000),
//      ("Jen","Finance","NY",7900),("Jeff","Marketing","CA",8000))
//    val df2 = data2.toDF("name","dept","state","salary")
//    df2.printSchema()
//
//    //root
//    // |-- name: string (nullable = true)
//    // |-- dept: string (nullable = true)
//    // |-- state: string (nullable = true)
//    // |-- salary: long (nullable = true)
//
//    val merged_cols = df1.columns.toSet ++ df2.columns.toSet
//
//    println(merged_cols)
//    import org.apache.spark.sql.functions.{col,lit}
//    def getNewColumns(column: Set[String], merged_cols: Set[String]) = {
//      merged_cols.toList.map(x =>
//        if (column.contains(x) )
//        {  col(x)}
//        else
//     {   lit(null).as(x)
//
//      })
//    }
//    println(getNewColumns(df1.columns.toSet, merged_cols))
//    val new_df1=df1.select(getNewColumns(df1.columns.toSet, merged_cols):_*)
//   val d= df1.select(col("name"),lit(null).as("s"))
//    d.show()
//    val new_df2=df2.select(getNewColumns(df2.columns.toSet, merged_cols):_*)

//    val spark = SparkSession
//      .builder()
//      .config(sparkconf)
//      .getOrCreate()

    import spark.implicits._

    // DataFrame 1
    val df1 = Seq(
      ("x", "bc"),
      ("x", "ce"),
      ("x", "ab"),
      ("x", "ef"),
      ("x", "gh"),
      ("y", "hk"),
      ("z", "jk")
    ).toDF()
    df1.show(10,false)

    //DataFrame2
    val df2 = Seq(
      ("x", "gkl"),
      ("y", "nmb"),
      ("z", "qwe")
    ).toDF()

    df2.show(10,false)

    // Method to eliminate data skewness
    def elimnateDataSkew(leftTable: DataFrame, leftCol: String, rightTable: DataFrame) = {

      var df1 = leftTable
        .withColumn(leftCol+"1", concat(
          leftTable.col(leftCol), lit("_"), lit(floor(rand(123456) * 10)))).drop(leftCol)
      var df2 = rightTable
        .withColumn("explodedCol",
          explode(
            array((0 to 10).map(lit(_)): _ *)
          ))

      (df1, df2)
    }

    val (df3, df4) = elimnateDataSkew(df1, "_1", df2)

    df3.show(100, false)
    df4.show(100, false)

    //join after elminating data skewness
   val dd= df3.join(
      df4,
      df3.col("_11")<=> concat(df4.col("_1"),lit("_"),df4.col("explodedCol")))

    dd.show()
     dd.withColumn("_11",split(dd.col("_11"),"_").getItem(0)).show()

  }
}
