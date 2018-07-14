package indi.bigdata.demo

/**
  * Created by xiwu on 18/6/12
  */

import org.apache.spark.sql.SparkSession

object WordCountSparkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WordCountDemo").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    val initRDD = sc.textFile("file:///Users/liebaomac/demo_wordcount/input/")
    initRDD.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_).collect().foreach(println _)
  }
}
