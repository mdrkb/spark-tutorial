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

    // Read Dataset from the a file
    val df = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ";")
      .csv("data/test_customer.csv")

    // Get column list from Dataset
    val columns = df.columns.toList

    // Iterate through each column of the DataSet
    columns.foreach(column => {
      val current_index = columns.indexOf(column)
      val current_column = column

      if (current_index + 1 < columns.length) {
        // Again iterate through the remaining columns of the Dataset
        for (next_index <- (current_index + 1) until columns.length) {
          val next_column = columns(next_index)

          // Check if the data types of two columns are same
          if (df.schema.fields(current_index).dataType == df.schema.fields(next_index).dataType) {
            //            println("Current column: " + current_column)
            //            println("Next column: " + next_column)

            // Get distinct column values
            val setA = df.select(current_column).map(_.get(0).toString).collect.toSet
            val setB = df.select(next_column).map(_.get(0).toString).collect.toSet

            // Check if a column is a subset of the other one
            println(current_column + " ⊆ " + next_column + " = " + setA.subsetOf(setB))
            println(next_column + " ⊆ " + current_column + " = " + setB.subsetOf(setA))

            //            println(setA)
            //            println(setB)
            //            println(setA.subsetOf(setB))
            //            println(setB.subsetOf(setA))
            //            println()
          }
        }
      }
    })


  }
}
