import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkTutorial {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create SparkSession
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]")
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    // Read Dataset from the file
    val df = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ";")
      .csv("data/test_customer.csv")

    val columns = df.columns.toList

    columns.foreach(column => {
      val current_column = column
      val current_index = columns.indexOf(current_column)

      if (current_index + 1 < columns.length) {
        for (next_column <- (current_index + 1) until columns.length) {
          println("Current column: " + current_column)
          println("Next column: " + columns(next_column))
          val listA = df.select(current_column).distinct.map(_.get(0).toString).collect.toList
          val listB = df.select(columns(next_column)).distinct.map(_.get(0).toString).collect.toList
          println(listA)
          println(listB)
          println()
        }
        println("--------------------------------------------------------------")
      }
    })


  }
}
