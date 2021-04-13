package com.jarvis.utils

import com.jarvis.utils.SparkUtils.schemaOf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe.{TypeTag, typeOf}

class DatasetFunctions[T](private val ds: Dataset[T]) extends AnyVal {
  import ds.sparkSession.implicits._

  /**
   * Remove duplicate rows using some column criteria for grouping and ordering
   *
   * @param partCols - How to group rows.  Only 1 row from each group will be in the result
   * @param order    - How to sort rows.  The row with the first order position will be kept
   * @param conv     - Optional: Encoder for T, allowing us to work with both Dataset and DataFrames
   * @return The data with only 1 row per group based on given sort order
   */
  def distinctRows(partCols: Seq[String], order: Seq[Column])(implicit conv: Encoder[T] = null): Dataset[T] = {
    val logicalOutput = ds.queryExecution.logical.output
    val sortOrder: Seq[SortOrder] = order.map(_.expr match {
      case expr: SortOrder =>
        val ref = logicalOutput.find(_.name == expr.references.head.name).orNull
        expr.copy(child = ref)
      case expr: Expression if expr.references.isEmpty =>
        SortOrder(expr, Ascending)
      case expr: Expression =>
        val ref = logicalOutput.find(_.name == expr.references.head.name).orNull
        SortOrder(ref, Ascending)
    })

    val groups = {
      val attribs = partCols.flatMap { (colName: String) =>
        logicalOutput.collect { case expr if expr.name == colName =>
          val ref = bindReference[Expression](expr, logicalOutput)
          Cast(ref, StringType)
        }
      }
      ConcatWs(Literal(",") +: attribs)
    }

    val sorter = new InterpretedOrdering(sortOrder, logicalOutput)
    val clean = ds.queryExecution.toRdd.keyBy(groups.eval)
      .reduceByKey { (a, b) => if (sorter.compare(a, b) > 0) b else a }
      .values

    val res = JarvisInternal.toDataframe(clean, ds.sparkSession, ds.schema)
    if (conv == null) res.asInstanceOf[Dataset[T]] else res.as[T](conv)
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
