package Dataframes
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object profit_status {

    def main(args: Array[String]): Unit = {
      // Create SparkSession
      val spark = SparkSession.builder()
        .appName("ProfitStatusExample")
        .master("local[*]") // use local mode for testing
        .getOrCreate()

      import spark.implicits._

      // Sample data
      val data = Seq(
        (1000.0, 800.0, 200.0),  // Profitable
        (500.0, 500.0, 0.0),     // Break-even
        (0.0, 1000.0, -1000.0),  // No Sales
        (2000.0, 2500.0, -500.0),// Loss-making
        (1500.0, 1200.0, 300.0)  // Profitable
      )

      // Create DataFrame
      val df = data.toDF("sales", "expenses", "profit")

      // Define profit_status calculation with conditions
      val profitStatusDf = df.withColumn(
        "profit_status",
        when(col("profit") > 0, "Profitable")
          .when(col("profit") === 0, "Break-even")
          .when(col("profit") < 0 && col("sales") > 0, "Loss-making")
          .when(col("profit") < 0 && col("sales") === 0, "No Sales")
          .otherwise("Unknown") // Optional: handle any unexpected cases
      )

      // Show the result
      profitStatusDf.show()

      // Stop SparkSession
      spark.stop()
    }
  }
