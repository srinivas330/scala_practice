
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.expressions
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, desc, substring}
object second_high_sal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]")
      .appName("SecondHighestSalary")
      .getOrCreate()
    import spark.implicits._
      val df=spark.read.option("header", "true").option("inferSchema", "true").csv("C:/Users/Niwas/Documents/Downloads/employees.csv")
      val df1=df.withColumn("Intial_name", substring(col("FIRST_NAME"), 1, 1))
    val windowSpec =Window.orderBy(desc("salary"))
    val rankedDf = df.withColumn("rank", dense_rank().over(windowSpec))
    val secondHighestSalaryDf = rankedDf.filter($"rank" === 1)
    println("Second Highest Salary:")
    secondHighestSalaryDf.show()
    scala.io.StdIn.readLine()
  }
}