package com.jarvis.utils

import com.jarvis.utils.SparkUtils.schemaOf
import com.jarvis.utils.compaction.Compactor
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.types._

import scala.annotation.tailrec
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
  @tailrec final def withColumns(newColTuples: (String, Column)*): DataFrame = {
    val allCols = ds.schema.fieldNames
    val forbiddenChars = Seq('#', '.', '\'')

    // reject special characters
    allCols.foreach(c => require(!c.exists(forbiddenChars.contains),
      s"""Columns containing ${forbiddenChars.toString()} are not allowed, and $c violates that."""))

    // for each column to update, we include all of those that don't have a reference to another that has to be updated first
    val (now, later) = {
      val seen = new scala.collection.mutable.HashSet[String]
      val references = newColTuples.map { case (name, definition) =>
        val refs = definition.expr.collectLeaves.map(_.toString).collect { case c if c.startsWith("'") => c.replaceAll("'", "") }.toSet
        (name, definition, refs)
      }
      references.partition {
        case (n, _, _) if seen.contains(n) => false // re-using a column already being added, so wait until next iteration
        case (n, _, refs) if refs.intersect(seen).isEmpty => // no overlap with others already being changed
          seen.add(n)
          true
        case (n, _, _) =>  // we've got to wait until next iteration
          seen.add(n)
          false
      }
    }

    val updatedCurrent = {
      val nowMap = now.map{case (n, d, _) => (n, d)}.toMap
      allCols.map(c => nowMap.getOrElse(c, col(c)) as c)
    }

    val toAddFromNow = {
      val allColsSet = allCols.toSet
      now.filterNot(n => allColsSet.contains(n._1)).map { case (name, newVal, _) => newVal as name }
    }

    val newCols = updatedCurrent ++ toAddFromNow
    val nextDf = ds.select(newCols:_*)
    if (later.isEmpty) nextDf else new DatasetFunctions[Row](nextDf).withColumns(later.map{ case (n, d, _) => (n, d)}:_*)
  }

  /**
   * Rename multiple new columns of the current DataFrame
   * (i.e., `withColumnRenamed` for a sequence of (String, String) Tuples).
   *
   * @param renameColTuples - a list of current, new column name Tuples2 (currColName: String, newColName: String).
   * @return - The DataFrame with mulitple renamed columns.
   */
  final def withColumnsRenamed(renameColTuples: (String, String)*): DataFrame = {
    val renameMap = {
      val colSet = ds.columns.toSet
      renameColTuples.filter(kv => colSet.contains(kv._1)).toMap
    }

    val deferred = renameColTuples.filterNot(kv => renameMap.contains(kv._1))
    val res = ds.select(ds.columns.map(c => renameMap.get(c).fold(col(c)){v => col(c) as v}):_*)

    if (deferred.nonEmpty) {
      new DatasetFunctions(res).withColumnsRenamed(deferred:_*)
    } else {
      res
    }
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

  /**
   * Creates a Compactor instance
   *
   * @return - Returns a compactor for writing this dataset
   */
  def compact: Compactor[T] = new Compactor[T](ds)

  /**
   * A test-only utility function that turns a Dataset/DataFrame into a typed Dataset with missing columns
   * being given a value of null. This will fail if required non-nullable fields are not provided.
   *
   * @note DO NOT USE IN PRODUCTION CODE. It hides a lot of potential bugs if you do.
   * @return - current Dataset as an instance of Dataset[U] with fields removed as necessary and missing ones added
   *         with null values.
   */
  def adopt[U: Encoder : TypeTag](implicit tp: TypeTag[T]): Dataset[U] = {
    val destSchema = schemaOf[U]
    val currSchema = ds.schema

    val resultColumns = destSchema.map { destField =>
      val lowerName = destField.name.toLowerCase
      val matching = {
        currSchema.find(f => f.name.toLowerCase == lowerName &&
          (f.dataType == destField.dataType || destField.dataType.isInstanceOf[StringType]
            || (destField.dataType.isInstanceOf[DecimalType] && f.dataType.isInstanceOf[DecimalType])))
      }

      matching.fold(lit(null).cast(destField.dataType) as destField.name) { f =>
        (if (f.dataType == destField.dataType) ds(f.name) else ds(f.name).cast(destField.dataType)) as destField.name
      }
    }

    ds.select(resultColumns:_*).as[U]
  }

  /**
   * Returns a string representing the case class for the present Dataset.
   * @param name - Name of the resulting case class
   * @return A String representing the case class, with the given name
   * @note Doesn't handle nested structs (for now)
   */
  def caseClass(name: String): String = {
    def typeString(dt: DataType): String = dt match {
      case ByteType | ShortType | FloatType | DoubleType | StringType | BooleanType | IntegerType =>
        dt.simpleString.capitalize
      case LongType => "Long"
      case _: ArrayType =>
        val e = dt match {
          case ArrayType(elemType, _) => elemType
        }
        s"Seq[${typeString(e)}]"
      case _: MapType =>
        val (k, v) = dt match {
          case MapType(keyType, valueType, _) => (keyType, valueType)
        }
        s"scala.collection.Map[${typeString(k)}, ${typeString(v)}]"
      case _: DecimalType => "java.math.BigDecimal"
      case _: BinaryType => "Array[Byte]"
      case _: DateType => "java.sql.Date"
      case _: TimestampType => "java.sql.Timestamp"
      case _: StructType => "org.apache.spark.sql.Row"
      case _ => "String"
    }

    def field(s: StructField): String = {
      val f = typeString(s.dataType)
      val name = s.name
      s match {
        case _ if f == "String" => s"$name: $f"
        case x if x.nullable && !f.exists(c => c == '.' || c == '[') => s"$name: Option[$f]"
        case _ => s"$name: $f"
      }
    }

    val sb = new StringBuilder
    var i = 0
    ds.schema.foreach { s =>
      if (i > 0) {
        sb ++= (if (i % 10 == 0) ",\n  " else ", ")
      }
      sb ++= field(s)
      i += 1
    }

    s"""case class $name(${sb.toString()})""".stripMargin
  }
}
