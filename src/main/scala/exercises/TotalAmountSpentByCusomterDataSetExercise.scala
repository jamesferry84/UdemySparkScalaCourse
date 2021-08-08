package exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

case class CustomerOrders(customerId: Int, productId: Int, amount: Float)

object TotalAmountSpentByCusomterDataSetExercise extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("TotalAmountDataSet")
    .master("local[*]")
    .getOrCreate()

  val customerOrderSchema = new StructType()
    .add("customerId", IntegerType, nullable = true)
    .add("productId", IntegerType, nullable = true)
    .add("amount", FloatType, nullable = true)

  import spark.implicits._
  val ds = spark.read
    .schema((customerOrderSchema))
    .csv(("data/customer-orders.csv"))
    .as[CustomerOrders]

  val groupedDataSet = ds.groupBy($"customerId").agg(round(sum("amount"), 2).alias("amount"))
  val sortedByAmountSpent = groupedDataSet.sort("amount")
  sortedByAmountSpent.show(sortedByAmountSpent.count().toInt)
}
