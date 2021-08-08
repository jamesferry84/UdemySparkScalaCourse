package exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, split, sum, min}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

case class SuperHeroName(id: Int, name: String)
case class ObscureSuperhero(value: String)

object MostObscureSuperheroExercise extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkSession = SparkSession
    .builder()
    .appName("obscureSuperHero")
    .master("local[*]")
    .getOrCreate()

  val superHeroNameSchema = new StructType()
    .add("id", IntegerType, true)
    .add("name", StringType, true)

  import sparkSession.implicits._
  val superHeroNamesDataSet = sparkSession.read
    .schema(superHeroNameSchema)
    .option("sep", " ")
    .csv("data/Marvel-names.txt")
    .as[SuperHeroName]

  val lines = sparkSession.read
    .text("data/Marvel-graph.txt")
    .as[ObscureSuperhero]

  val connections = lines
    .withColumn("id", split(col("value"), " ")(0))
    .withColumn("connections", size(split(col("value"), " ")) - 1)
    .groupBy("id").agg(sum("connections").alias("connections"))

  val minValue = connections.agg(min("connections")).first().getLong(0)
  val leastPopularHeroes = connections.filter($"connections" === minValue)

  val names = superHeroNamesDataSet.join(leastPopularHeroes, usingColumn = "id")

  names.show(names.count.toInt)

  println(s"Most obscure superhero is : ${names}")

}
