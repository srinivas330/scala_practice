package samples_

import org.apache.spark.SparkContext

object movie_rating {
def main(args: Array[String]):Unit={
  val sc=new SparkContext("local[*]","rating")
  val rdd1=sc.textFile("C:/Users/Niwas/Documents/Downloads/movies.txt")
  val rdd2=rdd1.map(x=>x.split(","))
  val rdd3=rdd2.map(x=>(x(1),x(2).toFloat))
  val rdd4=rdd3.map { case (x, y) => (x, (y, 1)) }
  //val rdd4=rdd3.reduceByKey((x,y)=>(x+y))
  val rdd5=rdd4.reduceByKey{ case ((a,b),(c,d))=> (a + c, b + d)}
 val rdd6=rdd5.map{ case (x, (a,b)) => (x, a/b)}
  val rdd7=rdd6.sortBy(x=>x._2,false)
  //rdd7.collect.foreach(println)
  rdd7.take(1).foreach(println)

}
}
