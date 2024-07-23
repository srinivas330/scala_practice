package samples

import org.apache.spark.SparkContext

import java.time.LocalDate
import java.time.format.DateTimeFormatter
object sales_od_product {
  // Define the date format and date range as static values
  val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val startDate = LocalDate.parse("2023-01-01", dateFormat)
  val endDate = LocalDate.parse("2023-03-31", dateFormat)

  // Function to check if a date is within the range
  def isDateInRange(date: String): Boolean = {
    val currentDate = LocalDate.parse(date, dateFormat)
    !currentDate.isBefore(startDate) && !currentDate.isAfter(endDate)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "sales_product")
    val rdd = sc.textFile("C:/Users/Niwas/Documents/Downloads/product.txt")
    //val rdd1= rdd.map(x => x.split(","))
    val rdd1=rdd.map(x => {val parts = x.split(",")
      (parts(0), parts(1), parts(2).toInt)
    })
    val rdd2= rdd1.filter { case (_,date,_) => isDateInRange(date) }
    val rdd3=rdd2.map{case (x,y,z)=>(x,z)}.reduceByKey((x,y)=>(x+y))

    rdd3.collect.foreach(println)

  }
}
