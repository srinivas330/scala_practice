package samples_

import org.apache.spark.SparkContext

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object stocks_top {
  // Define the date format and date range as static values
  val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val startDate = LocalDate.parse("2024-01-01", dateFormat)
  val endDate = LocalDate.parse("2024-01-31", dateFormat)

  // Function to check if a date is within the range
  def isDateInRange(date: String): Boolean = {
    val currentDate = LocalDate.parse(date, dateFormat)
    !currentDate.isBefore(startDate) && !currentDate.isAfter(endDate)
  }

  def main(args: Array[String]): Unit = {
    // Initialize Spark context
    val sc = new SparkContext("local[*]","Top5StocksByAveragePrice")
    val rdd=sc.textFile("C:/Users/Niwas/Documents/Downloads/sales.txt")
    // Sample input data

    // Create an RDD from the input data
   // val rdd = sc.parallelize(input)

    val rdd1=rdd.map(x=> {val parts=x.split(",")
      (parts(0), parts(1), parts(2).toFloat)})

    // Filter the data by date range using a serializable function
    val rdd2 = rdd1.filter { case (_, date, _) => isDateInRange(date) }

    // Map each record to a key-value pair where the key is the stock symbol and the value is the price
    val rdd3 = rdd2.map { case (stockSymbol, _, price) =>
      (stockSymbol, (price, 1))  // (price, 1) to count days
    }

    // Calculate the sum of prices and count of days for each stock
    val rdd4 = rdd3.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
      (sum1 + sum2, count1 + count2)
    }

    // Calculate the average daily price for each stock
    val rdd5 = rdd4.mapValues { case (sum, count) =>
      sum / count
    }

    // Sort the stocks by average daily price in descending order
    val rdd6 = rdd5.sortBy(_._2, ascending = false)

    // Take the top 5 stocks
    val rdd7 = rdd6.take(5)

    // Collect and print the results
    rdd7.foreach(println)

    // Stop the Spark context
    sc.stop()
  }
}
