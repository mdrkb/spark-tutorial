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
      val names = df.select(column).distinct.map(_.get(0).toString).collect.toList
      println(names)
    })



  }
}
