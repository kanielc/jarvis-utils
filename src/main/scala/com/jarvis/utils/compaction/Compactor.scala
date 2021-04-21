package com.jarvis.utils.compaction

import com.jarvis.utils.compaction.Compactor.{bestBlockCount, locateFolders}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.net.URI
import scala.util.Random

case class BucketSpecs(numBuckets: Int, colName: String, colNames: Seq[String])

class Compactor[T](ds: Dataset[T]) {
  private var writer = ds.write

  private var mode: String = SaveMode.ErrorIfExists.toString
  private var buckets: BucketSpecs = _
  private var format: String = "parquet"
  private var partitions: Seq[String] = _
  private var sortCols: Seq[String] = _
  private val properties: scala.collection.mutable.HashMap[String, String] = scala.collection.mutable.HashMap.empty

  def mode(s: SaveMode): this.type = {
    mode = s.toString
    this
  }

  /**
   *
   * @param s - mode to write with.  Adds an additional "part_overwrite" mode that just overwrites affected partitions (and leaves the rest)
   * @return
   */
  def mode(s: String): this.type = {
    mode = s.toLowerCase
    this
  }

  def bucketBy(numBuckets: Int, colName: String, colNames: String*): this.type = {
    buckets = BucketSpecs(numBuckets, colName, colNames)
    this
  }

  def format(s: String): this.type = {
    format = s
    this
  }

  def partitionBy(cols: String*): this.type = {
    partitions = cols
    this
  }

  def sortBy(col: String, cols: String*): this.type = {
    sortCols = col +: cols
    this
  }

  def option(key: String, value: String): this.type = {
    properties.update(key, value)
    this
  }

  def options(kv: Map[String, String]): this.type = {
    properties.clear()
    kv.foreach(rec => properties.put(rec._1, rec._2))
    this
  }

  def save(path: String): Unit = {
    val spark = ds.sparkSession

    val fs = FileSystem.get(new URI(path), spark.sparkContext.hadoopConfiguration)

    if (mode == "overwrite") fs.delete(new Path(path), true)
    // write the table

    val tempPath = s"/tmp/tempwritestore-${new String(Random.alphanumeric.take(16).toArray)}-${System.currentTimeMillis()}"

    val localFS = FileSystem.get(new URI(tempPath), spark.sparkContext.hadoopConfiguration)
    val destFS = FileSystem.get(new URI(path), spark.sparkContext.hadoopConfiguration)

    if (null == partitions || partitions.isEmpty) {
      ds.write.mode(mode).parquet(tempPath)
      spark.read.parquet(tempPath).repartition(bestBlockCount(tempPath, localFS, destFS))
        .write.mode(mode).options(properties).format(format).save(path)
    } else {
      ds.write.mode(mode).partitionBy(partitions:_*).parquet(tempPath)
      val suffix = format match {
        case "parquet" | "avro" | "orc" => format
        case _ => throw new RuntimeException("Compaction is only supported for parquet, avro and orc formats")
      }
      val dataPartitions: Seq[String] = locateFolders(tempPath, suffix, spark)
      val blockSize = Math.max(32 * 1024 * 1024, destFS.getDefaultBlockSize(null))

      dataPartitions.foreach { p =>
        spark.read.parquet(p).repartition(bestBlockCount(p, localFS, destFS)).write.mode(mode)
          .options(properties + ("parquet.block.size" -> blockSize.toString)).format(format)
          .save(p.replace(s"$tempPath", path.replaceFirst("file://", "")))
        fs.delete(new Path(p), true)
      }
    }
    fs.delete(new Path(tempPath), true)

    // add the _SUCCESS flag
    fs.create(new Path(s"$path/_SUCCESS"), true).close()
  }

  def saveAsTable(tableName: String): Unit = {

  }
}

object Compactor {
  def bestBlockCount(path: String, localFs: FileSystem, destFs: FileSystem): Int =  {
    val fsPaths = localFs.globStatus(new Path(path))
    val totalInputSize = fsPaths.foldLeft(0L)((curr, fsElmt) => curr + localFs.getContentSummary(fsElmt.getPath).getLength)
    val blockSize = Math.max(32 * 1024 * 1024, destFs.getDefaultBlockSize(null))
    Math.ceil(totalInputSize.toDouble / blockSize).toInt
  }

  def locateFolders(path: String, suffix: String, spark: SparkSession): Seq[String] = {
    val fs = FileSystem.get(new URI(path), spark.sparkContext.hadoopConfiguration)
    val p = new Path(path)
    fs.listStatus(p).flatMap { d =>
      if (d.isDirectory) locateFolders(d.getPath.toString, suffix, spark)
      else if (d.getPath.getName.endsWith(suffix)) Seq(d.getPath.getParent.toString)
      else Seq.empty }.distinct
  }
}
