package Dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object new_column {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()

    val schema=StructType(List(

      StructField("id",IntegerType,nullable =false),
      StructField("Name",StringType,nullable =false),
      StructField("Age",IntegerType,nullable =false),
      StructField("Salary",IntegerType,nullable =false),
      StructField("city",StringType,nullable =false),
      StructField("details",StringType,nullable =false),
      StructField("mean",IntegerType,nullable =false)

    ))
    val df=spark.read
      .format("csv")
      .option("header",true)
      .schema(schema)
      .option("path","C:/Users/Niwas/Documents/Downloads/info.csv")
      .load()
    val df1= df.withColumn("is_adult", col("age") >= 18)
    df1.show()


}}

