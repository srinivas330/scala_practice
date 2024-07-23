package setB

import org.apache.spark.SparkContext
import setA.repeated_words.rdd1

object setB_3 extends App {

  val sc= new SparkContext("local[*]","words")
  val rdd1=sc.textFile("C:/Users/Niwas/Documents/Downloads/review_text.txt")
  val rdd2=rdd1.flatMap(x=>x.split(",")).map(x=>(x,1)).reduceByKey((x,y)=>(x+y))
  val rdd3=rdd2.sortBy(x=>x._1,false)
  rdd3.collect.foreach(println)
  scala.io.StdIn.readLine()

}
