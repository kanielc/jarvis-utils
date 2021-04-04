package com.jarvis.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import scala.reflect.runtime.universe._

/* Several of these taken directly or with modifications from:
 https://github.com/kanielc/spark-utils/blob/master/src/main/scala/com/jakainas/functions/package.scala */
object SparkUtils {
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

  implicit class DatasetFunctions[T](private val ds: Dataset[T]) extends AnyVal {

    import ds.sparkSession.implicits._

    /**
     * Remove duplicate rows using some column criteria for grouping and ordering
     *
     * @param partCols - How to group rows.  Only 1 row from each group will be in the result
     * @param order    - How to sort rows.  The row with the first order position will be kept
     * @param conv     - Optional: Encoder for T, allowing us to work with both Dataset and DataFrames
     * @return The data with only 1 row per group based on given sort order
     */
    def distinctRows(partCols: Seq[Column], order: Seq[Column])(implicit conv: Encoder[T] = null): Dataset[T] = {
      var i = 0
      var colName = s"rn$i"

      while (ds.columns.contains(colName)) {
        i += 1
        colName = s"rn$i"
      }

      val deduped = ds.withColumn(s"rn$i", row_number over Window.partitionBy(partCols: _*)
        .orderBy(order: _*)).where(col(colName) === 1).drop(colName)

      if (conv == null) deduped.asInstanceOf[Dataset[T]] else deduped.as[T](conv)
    }

    /**
     * Compares two Spark DataTypes to determine if they are the same or not, ignoring nullability.
     */
    private def sameDataType(x: DataType, y: DataType): Boolean = {
      (x, y) match {
        case (a: ArrayType, b: ArrayType) => sameDataType(a.elementType, b.elementType)
        case (a: MapType, b: MapType) => sameDataType(a.keyType, b.keyType) & sameDataType(a.valueType, b.valueType)
        case (a: StructType, b: StructType) => a.fields.zip(b.fields).map { case (d, e) => sameDataType(d.dataType, e.dataType) }.reduce(_ & _)
        case (a: DataType, b: DataType) => a.typeName == b.typeName
      }
    }

    /**
     * Converts the dataframe into the schema matching U by selecting the fields and then calling `as`.
     * Throws an exception at run time if column types are incorrect, ignoring nullability.
     *
     * @tparam U - The type whose schema to align to.
     * @return a Dataset of type U.
     */
    def cast[U: Encoder: TypeTag]: Dataset[U] = {
      val expSchema = schemaOf[U]

      val expFields = expSchema.fields.sortBy(_.name)
      val expFieldNamesSet = expSchema.fieldNames.toSet
      val curFields = ds.schema.fields.filter(x => expFieldNamesSet.contains(x.name)).distinct.sortBy(_.name)

      val wrongDataTypes = expFields.zip(curFields).collect {
        case (x, y) if !sameDataType(x.dataType, y.dataType) => s"DataType for '${x.name}' column doesn't match expected DataType in '${typeOf[U].toString}' schema.\nExpected: ${x.dataType}\nReceived: ${y.dataType}"
      }.mkString("\n")

      if (wrongDataTypes.nonEmpty) {
        throw new IllegalArgumentException(wrongDataTypes)
      }

      val expFieldNames = expSchema.fieldNames
      ds.select(expFieldNames.head, expFieldNames.tail: _*).as[U]
    }

    /**
     * Add multiple new columns to the current DataFrame
     * (i.e., `withColumn` for a sequence of (String, Column) Tuples).
     *
     * @param newColTuples - a list of name-value Tuples2 (colName: String, colVal: Column).
     * @return - The DataFrame with new columns added.
     */
    def withColumns(newColTuples: (String, Column)*): DataFrame = newColTuples.foldLeft(ds.toDF()) {
      // From left to right, for each new (colName, colVal) Tuple add it to the current DataFrame
      case (newDF, (colName, colVal)) => newDF.withColumn(colName, colVal)
    }

    /**
     * Rename multiple new columns of the current DataFrame
     * (i.e., `withColumnRenamed` for a sequence of (String, String) Tuples).
     *
     * @param renameColTuples - a list of current, new column name Tuples2 (currColName: String, newColName: String).
     * @return - The DataFrame with mulitple renamed columns.
     */
    def withColumnsRenamed(renameColTuples: (String, String)*): DataFrame = renameColTuples.foldLeft(ds.toDF()) {
      // From left to right, for each new (currColName, newColName) Tuple apply withColumnRenamed
      case (newDF, (currColName, newColName)) => newDF.withColumnRenamed(currColName, newColName)
    }

    /**
     * Export a Dataset to a single CSV file (using Hadoop to copy the contents)
     *
     * @param path - location to write the single CSV file to, overwriting anything already there.
     */
    def saveCsv(path: String): Unit = {
      val tempPath = new Path(path + "-temp")
      val outFile = new Path(path)

      ds.write.option("header", "false").mode("overwrite").csv(tempPath.toString)

      val conf = ds.sparkSession.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(outFile.toUri, conf)
      fs.deleteOnExit(tempPath)

      val output = fs.create(outFile, true)

      // write header
      output.write(s"${ds.columns.mkString(",")}\n".getBytes("UTF-8"))

      // copy other files
      fs.listStatus(tempPath).foreach {
        case file if !file.isDirectory =>
          IOUtils.copyBytes(fs.open(file.getPath), output, conf, false)
        case _ => Unit
      }

      output.close()
      fs.close()
    }

    /**
     * Calculate the frequency of given columns and sort them in descending order
     *
     * @param  columns - column(s) to count by
     * @return - Dataframe with given columns soft by descending order of count
     */
    def countBy(columns: Column*): DataFrame = {
      ds.groupBy(columns: _*).agg(count("*") as "count").sort('count.desc)
    }

    /**
     * Calculate the frequency of given columns and sort them in descending order
     *
     * @param col1 - First column to group by
     * @param cols - remaining columns to count by (Optional)
     * @return - The DataFrame with columns and the respective frequency of the columns
     */
    def countBy(col1: String, cols: String*): DataFrame = {
      ds.groupBy(col1, cols: _*).agg(count("*") as "count").sort('count.desc)
    }
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
