import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

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

    // Get all CSV files
    val files = getListOfFiles("data")
    println(files)

    files.foreach(file => {
      println(file)
      // Get current file index
      val current_file_index = files.indexOf(file)

      // Read Dataset from the a file
      val df = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ";")
        .csv(file)

      // Get column list from Dataset
      val columns = df.columns.toList
      println(columns)

      // Iterate through each column of the DataSet
      columns.foreach(column => {
        val current_index = columns.indexOf(column)
        val current_column = column

        // Get distinct column values
        val setA = df.select(current_column).map(_.get(0).toString).collect.toSet
        val dataTypeSetA = df.schema.fields(current_index).dataType

        // Check inclusion dependency in same file
        if (current_index + 1 < columns.length) {
          // Again iterate through the remaining columns of the Dataset
          for (next_index <- (current_index + 1) until columns.length) {
            val next_column = columns(next_index)

            // Check if the data types of two columns are same
            if (dataTypeSetA == df.schema.fields(next_index).dataType) {
              val setB = df.select(next_column).map(_.get(0).toString).collect.toSet

              // Check if a column is a subset of the other one
              println(current_column + " ⊆ " + next_column + " = " + setA.subsetOf(setB))
              println(next_column + " ⊆ " + current_column + " = " + setB.subsetOf(setA))
            }
          }
        }

        // Check inclusion dependency in other files
        println("---------------------- Other Files -----------------------")
        println(current_column)
        findSubsetInOtherFiles(setA, dataTypeSetA, current_column, files, current_file_index, spark)
        println("---------------------- Other Files End -----------------------")
        println()
      })
      println("--------------------------------------------------------")
    })
  }

  def findSubsetInOtherFiles(setA: Set[String], dataTypeSetA: DataType, current_column: String, files: List[String], current_file_index: Int, spark: SparkSession): Unit = {
    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    for (next_file_index <- (current_file_index + 1) until files.length) {
      val next_file = files(next_file_index)
      println(next_file)

      // Read Dataset from the a file
      val df = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ";")
        .csv(next_file)

      // Get column list from Dataset
      val columns = df.columns.toList
      println(columns)

      columns.foreach(column => {
        val next_index = columns.indexOf(column)

        if (dataTypeSetA == df.schema.fields(next_index).dataType) {
          // Get distinct column values
          val setB = df.select(column).map(_.get(0).toString).collect.toSet

          // Check if a column is a subset of the other one
          println(current_column + " ⊆ " + column + " = " + setA.subsetOf(setB))
          println(column + " ⊆ " + current_column + " = " + setB.subsetOf(setA))
        }
      })
    }
  }

  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .map(_.getPath).toList
  }
}
