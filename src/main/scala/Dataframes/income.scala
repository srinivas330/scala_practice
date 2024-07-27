package Dataframes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object income {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Income Categorization")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    // Sample data
    val data = Seq(
      (25000, "Alice"),
      (50000, "Bob"),
      (80000, "Charlie"),
      (null, "David"),
      (70000, "Eve"),
      (29000, "Frank")
    )
    // Create DataFrame
    val df = data.toDF("income", "name")

    // Define the conditions and create the new column "income_category"
    val dfWithIncomeCategory = df.withColumn("income_category",
      when(col("income").isNull, "Unknown")
        .when(col("income") < 30000, "Low")
        .when(col("income").between(30000, 70000), "Medium")
        .otherwise("High")
    )
    // Show the result
    dfWithIncomeCategory.show()

  }
}
