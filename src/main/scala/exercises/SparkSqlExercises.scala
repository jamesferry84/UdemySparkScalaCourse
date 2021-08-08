package exercises

import org.apache.spark.sql._
import org.apache.log4j._


object SparkSqlExercises extends App {

  import org.apache.spark

  case class Person(id:Int, name:String, age: Int, friends: Int)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val ss = SparkSession
    .builder()
    .appName("SparkSQL")
    .master("local[*]")
    .getOrCreate()

  import ss.implicits._
  val people = ss.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/fakefriends.csv")
    .as[Person]

  people.printSchema()

//  people.createOrReplaceTempView("people")

  val columns = people.select("age", "friends")
  val averageAge = columns.groupBy("age").avg("friends")

//  people.filter(x => x.age < 21).show()
//  people.groupBy("age").count().show()
 // val teenagers = ss.sql("SELECT * FROM people WHERE age >=13 and age <= 19")

 val results = averageAge.collect()

  results.foreach(println)


  ss.stop()

}
