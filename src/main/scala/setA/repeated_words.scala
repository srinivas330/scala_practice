package setA

import org.apache.spark.SparkContext

object repeated_words extends App{

  val sc= new SparkContext("local[*]","words")
  val rdd1=sc.textFile("C:/Users/Niwas/Documents/Downloads/text.txt")
  val rdd2=rdd1.map(x => x.split(","))
  val rdd3=rdd2.map(x=>(x(1)))
  val rdd4=rdd3.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((x,y)=>(x+y))
  val rdd5=rdd4.sortBy(x=>x._2,false)
  rdd5.take(1).foreach(println)
  scala.io.StdIn.readLine()

}
