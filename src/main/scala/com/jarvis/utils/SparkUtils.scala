package com.jarvis.utils

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

/* Several of these taken directly or with modifications from:
 https://github.com/kanielc/spark-utils/blob/master/src/main/scala/com/jakainas/functions/package.scala */
object SparkUtils {
  implicit def ds2DatasetFunctions[T](ds: Dataset[T]): DatasetFunctions[T] = new DatasetFunctions(ds)

  /**
   * Returns a default value for a given column if it's null, otherwise return the column's value
   *
   * @param c       - Column to be checked
   * @param default - Value to return if `c` is null
   * @return c if not null and default otherwise
   */
  def nvl[C1](c: C1, default: Any)(implicit ev: C1 => Column): Column = when(c.isNotNull, c).otherwise(default)

  /**
   * Provides a literal of String with a value of null
   *
   * @return `null` as a column of type String
   */
  def null_string: Column = lit(null.asInstanceOf[String])

  /**
   * Returns the schema for a type (e.g. case class, string, etc)
   *
   * @tparam T - The type whose schema is to be returned
   * @return The schema of a Dataset built with that type
   */
  def schemaOf[T: TypeTag]: StructType = {
    val dataType = ScalaReflection.schemaFor[T].dataType
    dataType.asInstanceOf[StructType]
  }

  /**
   * countDistinct equivalent for window operations.
   * Distinct window operations are not currently supported by Spark so we can't just use countDistinct.
   *
   * TODO: see if performance can be improved using a UDAF or some other approach.
   *
   * @param countCol : Column values to count.
   * @param partCols : One or more columns to partition the dataset by.
   * @return : Count of unique values in countCol per partitionCols.
   */
  def countUnique(countCol: Column, partCols: Column*): Column = {
    size(collect_set(countCol).over(Window.partitionBy(partCols: _*)))
  }

  /**
   * Converts a timestamp into bigint (long) in milliseconds (default Spark returns only seconds)
   *
   * @param timestamp - Timestamp to extract milliseconds from
   * @return Milliseconds since epoch for given timestamp
   */
  def millis(timestamp: Column): Column = (timestamp.cast("double") * 1000).cast("bigint")

  /**
   * Generate a string Date column by combining year, month, and day columns.
   * If one or more column is null, the result Date will be null.
   *
   * @param year  - the year column
   * @param month - the month column
   * @param day   - the day column
   * @return a string column consist of the combination of year, month, and day into date
   */
  def to_date_str(year: Column, month: Column, day: Column): Column = {
    date_format(concat(year, lit("-"), month, lit("-"), day), "yyyy-MM-dd")
  }

  implicit class SparkFunctions(val spark: SparkSession) extends AnyVal {
    /**
     * Loads CSV file(s) from a given location, including header and inferring schema
     *
     * @param paths - single or multiple file locations to load CSV data from
     * @return Dataset[Row] containing the data in the CSVs
     */
    def readCsv(paths: String*): Dataset[Row] = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(paths: _*)
  }
}
