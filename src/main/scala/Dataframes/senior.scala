package Dataframes
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
object senior {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("AgeGroupExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data
    val data = Seq(
      (25, 50000),
      (35, 70000),
      (45, 60000),
      (65, 80000))

    // Create DataFrame
    val df = data.toDF("age", "income")
    df.show()
    // Define age group conditions
    val ageGroupColumn = when(col("age").isNull, "Unknown")
      .when(col("age") < 30, "Young")
      .when(col("age").between(30, 60), "Adult")
      .otherwise("Senior")

    // Add the age_group column to the DataFrame
    val dfWithAgeGroup = df.withColumn("age_group", ageGroupColumn)
    dfWithAgeGroup.show()

  }

}
