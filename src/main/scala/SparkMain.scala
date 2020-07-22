import org.apache.log4j.LogManager
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

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

  val csvDf = spark
    .read
    .format("csv")
    .options(
      Map("sep" -> ",",
        "header" -> "true",
        "quote" -> "'"))
    .load(csvFilePath)

  val jsonDf = spark
    .read
    .option("multiLine", value = true)
    .json(jsonFilePath)


  val hasEmptyValues = csvDf.columns.map(x => trim(col(x)) =!= "" || col(x).isNull).reduce(_ && _)

  val csvDfFiltered = csvDf.filter(hasEmptyValues)

  val modifications: List[ColumnProcessorAttrs] = jsonDf
    .as[ColumnProcessorAttrs]
    .collect()
    .toList

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

  val renamedColumnsDf = morphByAttrs(csvDfFiltered, modifications) match {
    case Success(resultDf) => resultDf
    case Failure(transformationException) => {
      log.error(s"Failed to transform DF using input provided.")
      throw new RuntimeException(transformationException)
    }
  }

  val onlyNewColumnsDf = renamedColumnsDf
    .select(modifications.map(_.new_col_name).map(col):_*)
}

case class ColumnProcessorAttrs(existing_col_name: String, new_col_name: String, new_data_type: String, date_expression: Option[String] = None)
