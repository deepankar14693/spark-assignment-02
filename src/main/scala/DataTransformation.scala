import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime



object DataTransformation extends App {
 val conf = new SparkConf()
 conf.setMaster("local")
 conf.setAppName("Word Count")
 val sc = new SparkContext(conf)
 val rdd1 = sc.textFile("/home/knoldus/Desktop/file1")
 val rdd2 = sc.textFile("/home/knoldus/Desktop/file2")
 //val now = rdd1.flatMap(x => x.split(" ")).map(value => (value,1)).reduceByKey(_ + _)
 //now.collect.foreach(println)
 val customerRdd = rdd1.map(x => x.split("#"))
 val convert = rdd2.map(x => x.split("#"))
 val custom: RDD[(Int, Int, Int, Int, Int)] = convert.map(x => {
                       val dateTime = new DateTime(x(0).toLong * 1000L)
  (dateTime.getYear,dateTime.getMonthOfYear,dateTime.getDayOfMonth,x(1).toInt,x(2).toInt)
 })

 custom.collect.foreach(println)

// val queryResult = sc.sql("select encounter.Member_ID AS patientID, encounter.Encounter_DateTime AS date, diag.code from encounter join diag on encounter.Encounter_ID = diag.Encounter_ID")
// val rdd: RDD[Row] = queryResult.rdd

}
