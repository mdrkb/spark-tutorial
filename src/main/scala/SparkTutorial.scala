import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkTutorial {
  def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //--------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //--------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    println("-----------------------------------------------------------------------------------------------")

    //--------------------------------------------------------------------------------------------------------
    // Loading data
    //--------------------------------------------------------------------------------------------------------

    // Create a Dataset programmatically
    val numbers = spark.createDataset((0 until 100).toList)

    // Read a Dataset from a file
    val customers = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ";")
      .csv("data/tpch_customer.csv") // also text, json, jdbc, parquet
      .as[(Int, String, String, Int, String, String, String, String)]

    println("-----------------------------------------------------------------------------------------------")

    //--------------------------------------------------------------------------------------------------------
    // Basic transformations
    //--------------------------------------------------------------------------------------------------------

    // Basic transformations on datasets return new datasets
    val mapped = numbers.map(i => "This is a number: " + i)
    val filtered = mapped.filter(s => s.contains("1"))
    val sorted = filtered.sort()
    List(numbers, mapped, filtered, sorted).foreach(dataset => println(dataset.getClass))
    sorted.show()

    println("-----------------------------------------------------------------------------------------------")

    // Basic terminal operations
    val collected = filtered.collect() // collects the entire dataset to the driver process
    val reduced = filtered.reduce((s1, s2) => s1 + "," + s2) // reduces all values successively to one
    filtered.foreach(s => println(s)) // performs an action for each element (take care where the action is evaluated!)
    List(collected, reduced).foreach(result => println(result.getClass))

    println("-----------------------------------------------------------------------------------------------")

    // DataFrame and Dataset
    val untypedDF = numbers.toDF() // DS to DF
    val stringTypedDS = untypedDF.map(r => r.get(0).toString) // DF to DS via map
    val integerTypedDS = untypedDF.as[Int] // DF to DS via as() function that cast columns to a concrete types
    List(untypedDF, stringTypedDS, integerTypedDS).foreach(result => println(result.head.getClass))
    List(untypedDF, stringTypedDS, integerTypedDS).foreach(result => println(result.head))

    println("-----------------------------------------------------------------------------------------------")

    // Mapping to tuples
    numbers
      .map(i => (i, "nonce", 3.1415, true))
      .take(10)
      .foreach(println(_))

    println("-----------------------------------------------------------------------------------------------")

    // SQL on DataFrames
    customers.createOrReplaceTempView("customers") // make this dataframe visible as a table
    val sqlResult = spark.sql("SELECT * FROM customers WHERE C_NATIONKEY = 15") // perform an sql query on the table

    import org.apache.spark.sql.functions._

    sqlResult // DF
      .as[(Int, String, String, Int, String, String, String, String)] // DS
      .sort(desc("C_NATIONKEY")) // desc() is a standard function from the spark.sql.functions package
      .head(10)
      .foreach(println(_))

    println("-----------------------------------------------------------------------------------------------")

    // Grouping and aggregation for Datasets
//    val topEarners = customers
//      .groupByKey { case (name, age, salary, company) => company }
//      .mapGroups { case (key, iterator) =>
//        val topEarner = iterator.toList.maxBy(t => t._3) // could be problematic: Why?
//        (key, topEarner._1, topEarner._3)
//      }
//      .sort(desc("_3"))
//    topEarners.collect().foreach(t => println(t._1 + "'s top earner is " + t._2 + " with salary " + t._3))

    println("-----------------------------------------------------------------------------------------------")

  }
}
