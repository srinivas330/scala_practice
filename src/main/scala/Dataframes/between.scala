package Dataframes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object between {
  def main(args: Array[String]): Unit = {


    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Grade Categorization")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data
    val data = Seq(
      (95, "Math"),
      (82, "Science"),
      (67, "English"),
      (78, "History"),
      (50, "Geography"),
      (91, "Physics")
    )

    // Create DataFrame
    val df = data.toDF("score", "subject")

    // Define the conditions and create the new column "grade"
    val dfWithGrade = df.withColumn("grade",
      when(col("score") >= 90, "A")
        .when(col("score").between(70, 89), "B")
        .otherwise("C")
    )

    // Show the result
    dfWithGrade.show()


  }
}