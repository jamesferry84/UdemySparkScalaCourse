package exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object TotalAmountSpentByCustomerExercise extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "TotalAmountSpentByName")
  val input = sc.textFile("data/customer-orders.csv")

  def readLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amountSpent = fields(2).toFloat
    (customerId,amountSpent)
  }

  val parsedLines = input.map(readLine)

  val totalAmountByCustomer = parsedLines.reduceByKey((x,y) => (x + y))

  val sortedByAmountSpent = totalAmountByCustomer.map(x => (x._2, x._1)).sortByKey()

  val result = sortedByAmountSpent.collect()

  result.foreach(println)

}
