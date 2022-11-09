package com.jarvis.utils.compaction

import com.jarvis.utils.SparkTest
import com.jarvis.utils.SparkUtils._
import com.jarvis.utils.compaction.CompactorTest.TestData
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import java.io.File

object CompactorTest {
  case class TestData(x: Int, y: String)
}

class CompactorTest extends SparkTest {
  import spark.implicits._

  test("can call parameters") {
    val ds = Seq(TestData(7, "a"), TestData(9, "b")).toDS

    val c = new Compactor[TestData](ds)

    val transformed = c.bucketBy(3, "a").sortBy("b").format("hive").mode("Overwrite")
      .option("useless", "true").options(Map("a" -> "true", "b" -> "true")).partitionBy("a", "b")

    transformed.getClass shouldEqual c.getClass
  }

  test("trivial compaction") {
    val file = "/tmp/test.parquet"
    FileUtils.deleteQuietly(new File(file))
    val ds = spark.range(1000)
    ds.compact.save(file)
    spark.read.parquet(file).count() shouldEqual 1000
    FileUtils.deleteQuietly(new File(file))
  }

  test("trivial compaction to hive") {
    spark.sql("DROP TABLE IF EXISTS test_foo")
    val ds = spark.range(1000)
    // overwrite
    ds.compact.mode("overwrite").saveAsTable("test_foo")
    spark.read.table("test_foo").count() shouldEqual 1000
    spark.sql("DROP TABLE IF EXISTS test_foo")
  }

  test("trivial compaction with partitioning") {
    val file = "/tmp/test.parquet"
    FileUtils.deleteQuietly(new File(file))
    val ds = spark.range(1000).withColumn("a", explode(array(lit("a1"), lit("a2"))))
    ds.compact.partitionBy("a").save(file)
    val back = spark.read.parquet(file)

    back.count() shouldEqual 2000

    // should have 2 partitions in there
    Compactor.locateFolders("/tmp/test.parquet", "parquet", spark).foreach(println)
    Compactor.locateFolders("/tmp/test.parquet", "parquet", spark).size shouldEqual 2
    FileUtils.deleteQuietly(new File(file))
  }

  test("trivial compaction with partitioning to hive") {
    spark.sql("DROP TABLE IF EXISTS test_foo")
    val ds = spark.range(1000).withColumn("a", explode(array(lit("a1"), lit("a2"))))
    ds.compact.partitionBy("a").saveAsTable("test_foo")
    val back = spark.read.table("test_foo")

    back.count() shouldEqual 2000

    // should have 2 partitions in there
    spark.table("test_foo").inputFiles.distinct.length shouldEqual 2
    spark.sql("DROP TABLE IF EXISTS test_foo")
  }

  test("handles casing") {
    val df = Seq(("a", "b", 7)).toDF("a", "b", "c")

    df.write.mode("overwrite").saveAsTable("test_foo")
    Seq(("a", "b", 7)).toDF("A", "B", "C").compact.mode("append").saveAsTable("test_foo")

    spark.table("test_foo").collect() should contain theSameElementsAs Array(Row("a", "b", 7), Row("a", "b", 7))
  }

}
