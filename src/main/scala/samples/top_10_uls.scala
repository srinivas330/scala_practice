package samples

import org.apache.spark.SparkContext

object top_10_uls {
  def main(args: Array[String]): Unit = {
    val sc=new SparkContext("local[*]","url")
    val rdd1=sc.textFile("C:/Users/Niwas/Documents/Downloads/urls.csv")
    val rdd2=rdd1.map(x=>x.split(","))
    val rdd3=rdd2.map(x=>(x(2),1))
    val rdd4=rdd3.reduceByKey((x,y)=>(x+y))
    val rdd5=rdd4.sortBy(x=>x._2,false)
    rdd5.collect.foreach(println)
    scala.io.StdIn.readLine()
  }
}
