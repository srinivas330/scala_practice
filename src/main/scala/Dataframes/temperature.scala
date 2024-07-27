package Dataframes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object temperature {
  def main(args: Array[String]): Unit = {
      // Create SparkSession
      val spark = SparkSession.builder()
        .appName("WeatherTypeExample")
        .master("local[*]") // use local mode for testing
        .getOrCreate()

      import spark.implicits._

      // Sample data
      val data = Seq(
        (35, 65),
        (25, 70),
        (32, 50),
        (28, 55),
        (22, 45),
        (30, 60)
      )

      // Create DataFrame
      val df = data.toDF("temperature", "humidity")

      // Define conditions for the new column
      val weatherTypeDf = df.withColumn("weather_type",
        when(col("temperature") >= 30 && col("humidity") >= 60, "Hot and Humid")
          .when(col("temperature") < 30 && col("humidity") >= 60, "Warm and Humid")
          .when(col("temperature") >= 30 && col("humidity") < 60, "Hot and Dry")
          .otherwise("Moderate")
      )

      // Show the result
      weatherTypeDf.show()

      // Stop SparkSession
      spark.stop()
    }
}
