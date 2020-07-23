import org.apache.log4j.LogManager
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.{Failure, Success, Try}

//App is developed to launch using spark2-submit (jar built by sbt package), all IO is made using FS (local or hdfs).
//Test files are available in src/test/resources
//launch example with args passed via spark.config:
//spark2-submit  --conf spark.csvloader.json_path="processing_attrs.json" --conf spark.csvloader.output_path="result.json" --conf spark.csvloader.csv_path="sample.csv" --class=SparkMain --verbose csvloader_2.11-1.0.jar

object SparkMain extends App {

  val spark = SparkSession
    .builder()
    .appName("CsvLoader")
    .getOrCreate()

  val sc = spark.sparkContext

  val log = LogManager.getRootLogger

  import spark.implicits._

  //path to csv is passed to spark-submit as --conf spark.csvloader.csv_path = "_path_to_csv_"
  val csvFilePath = spark.conf.get("spark.csvloader.csv_path")
  //path to params json is passed to spark-submit as --conf spark.csvloader.json_path = "_path_to_json_"
  val jsonFilePath = spark.conf.get("spark.csvloader.json_path")
  //path to result json is passed to spark-submit as --conf spark.csvloader.output_path = "_path_to_result_"
  val outputFilePath = spark.conf.get("spark.csvloader.output_path")

  //read input CSV data
  val csvDf = spark
    .read
    .format("csv")
    .options(
      Map("sep" -> ",",
        "header" -> "true",
        "quote" -> "'"))
    .load(csvFilePath)

  //read input rules for data modifications
  val jsonDf = spark
    .read
    .option("multiLine", value = true)
    .json(jsonFilePath)


  //create complex condition to filter rows with empty values
  val hasEmptyValues = csvDf.columns.map(x => trim(col(x)) =!= "" || col(x).isNull).reduce(_ && _)

  //filter DF
  val csvDfFiltered = csvDf.filter(hasEmptyValues)
  //parse modification rules into case class objects
  val modifications: List[ColumnProcessorAttrs] = jsonDf
    .as[ColumnProcessorAttrs]
    .collect()
    .toList

  //method to modify DF with some exception handling
  def morphByAttrs(inputDf: DataFrame, columnProcessorAttrs: List[ColumnProcessorAttrs]): Try[DataFrame] = Try {
    if (columnProcessorAttrs.isEmpty) {
      inputDf
    } else {
      val curColumnProcessorAttrs = columnProcessorAttrs.head
      val targetDataType = CatalystSqlParser.parseDataType(curColumnProcessorAttrs.new_data_type)
      val modifiedDf = targetDataType match {
        case date: DateType => inputDf.withColumn(curColumnProcessorAttrs.new_col_name, to_date(col(curColumnProcessorAttrs.existing_col_name), curColumnProcessorAttrs.date_expression.getOrElse("dd-MM-yyyy")))
        case _ => inputDf.withColumn(curColumnProcessorAttrs.new_col_name, col(curColumnProcessorAttrs.existing_col_name).cast(targetDataType))
      }

      val result = morphByAttrs(modifiedDf, columnProcessorAttrs.tail) match {
        case Success(df) => df
        case Failure(exception) => {
          log.error(exception)
          inputDf.withColumnRenamed(curColumnProcessorAttrs.existing_col_name, curColumnProcessorAttrs.new_col_name)
        }
      }
      result
    }
  }

  //transform out data
  val renamedColumnsDf = morphByAttrs(csvDfFiltered, modifications) match {
    case Success(resultDf) => resultDf
    case Failure(transformationException) => {
      log.error(s"Failed to transform DF using input provided.")
      throw new RuntimeException(transformationException)
    }
  }
  //take only columns that has modification rules
  val onlyNewColumnsDf = renamedColumnsDf
    .select(modifications.map(_.new_col_name).map(col): _*)

  //method to create statistics on given DF
  def createStatisticsDf(initialDf: DataFrame): DataFrame = {
    val columns = initialDf.columns
    val statisticSchema = new StructType()
      .add("Column", StringType, nullable = true)
      .add("Unique_values", LongType, nullable = true)
      .add("Values", MapType(StringType, LongType, valueContainsNull = false), nullable = true)
    var statisticsDf = spark.createDataFrame(sc.emptyRDD[Row], statisticSchema)
    for (column <- columns) {
      val mapFromColumnValues = initialDf
        .select(column)
        .groupBy(column)
        .agg(count(column).as("entries"))
        .distinct
        .na.drop
        .rdd
        .map(row => row.get(0).toString -> row.getLong(1))
        .collectAsMap

      val columnAndCountDf = initialDf
        .select(
          lit(column).as("Column"),
          countDistinct(column).as("Unique_values"),
          typedLit(mapFromColumnValues).as("Values"))

      statisticsDf = statisticsDf.union(columnAndCountDf)
    }
    statisticsDf
  }

  //get statistics for our DF
  val resultDf = createStatisticsDf(onlyNewColumnsDf)
  //spark out of the box doesn't support multiline JSON on writing, so to create exact output it will require some 3rd party library magic
  //e.g. we use circe for such tasks, but as i've got it is not the goal of the task
  resultDf
    .coalesce(1)
    .write
    .mode("overwrite")
    .json(outputFilePath)
}

case class ColumnProcessorAttrs(existing_col_name: String, new_col_name: String, new_data_type: String, date_expression: Option[String] = None)
