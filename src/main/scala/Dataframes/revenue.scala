package Dataframes
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object revenue {
    def main(args: Array[String]): Unit = {
      // Create SparkSession
      val spark = SparkSession.builder()
        .appName("TotalRevenueExample")
        .master("local[*]") // use local mode for testing
        .getOrCreate()

      import spark.implicits._

      // Sample data
      val data = Seq(
        (1, 10, 20.0),
        (2, 5, 15.0),
        (3, -1, 20.0), // quantity is negative
        (4, 8, -5.0), // price is negative
        (2, 2, 15.0),
        (1, 3, 20.0)
      )

      // Create DataFrame
      val df = data.toDF("product_id", "quantity", "price")

      // Define revenue calculation with conditions
      val revenueDf = df.withColumn(
        "revenue",
        when(col("product_id").isNull || col("quantity") < 0 || col("price") < 0, 0)
          .otherwise(col("quantity") * col("price"))
      )

      // Group by product_id and calculate total revenue
      val totalRevenueDf = revenueDf.groupBy("product_id")
        .agg(sum("revenue").alias("total_revenue"))

      // Show the result
      totalRevenueDf.show()

      // Stop SparkSession
      spark.stop()
    }
  }



