import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import AugmentationSpark_assist._

object AugmentationSpark {
  def main(args: Array[String]) = {

  def augment(spark: SparkSession, inputDetailPath: String, outputPath: String,
              timeIgnorance: Int = 5, breakPoint: Int = 1800): Unit = {


    import spark.implicits._
    


    val data = spark.read
      .format("csv")
      .option("ignoreLeadingWhiteSpace", true)
      .option("header",true)
      .load(inputDetailPath)

    val df: DataFrame = data
      .withColumn("time_seq", split(col("timeline"), "=>").cast("array<int>"))
      .withColumn("time_on_page", udf(timeOnPage _).apply(col("time_seq"), lit(timeIgnorance)))
      .withColumn("session_mask", udf(sessMask _).apply(col("time_on_page"), lit(breakPoint)))
      .withColumn("session_num", udf(sessNum _).apply(col("session_mask")))
      .withColumn("chain_mask", udf(chainMask _).apply(col("session_num")))

    val augmented = df
      .withColumn("categorical_path",
        udf(toCategorical _).apply(
          col("path"),
          col("session_num"),
          col("time_on_page")
        )
      )

    augmented
      .write
      .format("csv")
      .option("header", "true")
      .save(outputPath)
  }


    val spark = SparkSession.builder().appName("Data Augmentation").getOrCreate()


    augment(spark, "/Users/germangerken/Documents/test/test.csv", "/Users/germangerken/Documents/test/output", 5, 1800)

  }
}
