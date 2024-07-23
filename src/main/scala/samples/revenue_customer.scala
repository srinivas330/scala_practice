package samples

import org.apache.spark.SparkContext


object revenue_customer extends App{
  val sc=new SparkContext("local[*]","revenue")
  val rdd1 =sc.textFile("C:/Users/Niwas/Documents/Downloads/orders_revenue.csv")
  val rdd2=rdd1.map(x => x.split(","))
  val rdd3=rdd2.map(x =>(x(1),x(3).toFloat))
  val rdd4=rdd3.reduceByKey((x,y)=>x+y)
  rdd4.collect.foreach(println)
  scala.io.StdIn.readLine()
}
