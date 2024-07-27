package Dataframes
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object df_first {

  def main(args: Array[String]): Unit = {

      val spark=SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()


      //    val sparkconf=new SparkConf()
      //        sparkconf.set("spark.app.name","spark-program")
      //        sparkconf.set("spark.master","local[*]")
      //
      //      val spark=SparkSession.builder()
      //        .config(sparkconf)
      //        .getOrCreate()


      //    val schema=" id Int,Name String,Age Int,Salary Int,city String,details String,mean Int"

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

      df.show(false)
      df.printSchema()


      df.write
        .format("csv")
        .mode(SaveMode.Ignore)
        .option("path","C:/Users/Niwas/Documents/Downloads/info.csv")
        .save()




    }
  }

