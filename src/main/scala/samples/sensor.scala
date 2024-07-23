package samples

import org.apache.spark.SparkContext

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object sensor {
  // Define the date format and date range as static values
  val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val startDate = LocalDate.parse("2024-01-01", dateFormat)
  val endDate = LocalDate.parse("2024-04-31", dateFormat)

  // Function to check if a date is within the range
  def isDateInRange(date: String): Boolean = {
    val currentDate = LocalDate.parse(date, dateFormat)
    !currentDate.isBefore(startDate) && !currentDate.isAfter(endDate)
  }
  def main (args:Array[String]): Unit = {
 val sc=new SparkContext("local[*]","sensor")
 val rdd1=sc.textFile("C:/Users/Niwas/Documents/Downloads/sensor.csv")
  val rdd2=rdd1.map(x=> {val parts=x.split(",")
    (parts(0).toInt, parts(1), parts(2).toFloat)})

    val rdd3= rdd2.filter { case (_,date,_) => isDateInRange(date) }
   // val rdd3=rdd2.map(x=>(x(0),x(2).toDouble))
    //val rdd4=rdd3.map(x=>(x(0),1))
    val rdd4=rdd3.map { case (x,y,z) => (x,(z, 1)) }
    val rdd5=rdd4.reduceByKey{ case ((a,b),(c,d))=> (a + c, b + d)}
    val rdd6=rdd5.map{ case (x, (a,b)) => (x, a/b) }

   // val sensorAggregates= rdd2.map(x => (x._1, (x._3, 1)))
 //.reduceByKey { (accum, value) => (accum._1 + value._1, accum._2 + value._2)}
  //val sensorAverages: RDD[(Int, Double)] = sensorAggregates
  // .map(record => (record._1, record._2._1 / record._2._2))
     rdd6.collect.foreach(println)
  scala.io.StdIn.readLine()
}
}
