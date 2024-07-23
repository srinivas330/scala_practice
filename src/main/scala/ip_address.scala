import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
object ip_address extends App{

 // val saprk = SparkSession.builder().master("local[*]").appName("ip_address").getOrCreate()
  val sc= new SparkContext("local[*]","spark_1")
  val rawData = List(
    "192.186.1.1/192.186.1.2/192.186.1.1/192.186.1.3",
    "192.186.1.3/192.186.1.2/192.186.1.5/192.186.1.3",
    "192.186.1.3/192.186.1.5/192.186.1.1/192.186.1.3")
  val rdd1= sc.parallelize(rawData)
  val rdd2=rdd1.flatMap(x => x.split("/"))
  val rdd3=rdd2.map(x =>(x,1))
  val rdd4=rdd3.reduceByKey((x,y)=>x+y)
  val rdd5=rdd4.sortBy(x=>x._2,false)
  //val rdd5=rdd4.sortBy(x=>x._2,false)
  //rdd5.collect.foreach(println)
  rdd5.take(1).foreach(println)
  scala.io.StdIn.readLine()


}
