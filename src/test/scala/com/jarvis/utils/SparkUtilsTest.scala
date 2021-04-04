package com.jarvis.utils

import com.jarvis.utils.SparkUtils._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row

import java.io.File

class SparkUtilsTest extends SparkTest {
  import SparkUtilsTest._
  import org.apache.spark.sql.functions._
  import spark.implicits._

  test("distinct by removes duplicates") {
    // with a dataset
    Seq("a", "b", "a").toDS().distinctRows(Seq('value), Seq(lit(true)))
      .as[String].collect should contain theSameElementsAs Array("a", "b")

    // with a dataframe
    Seq("a", "b", "a").toDF("value").distinctRows(Seq('value), Seq(lit(true)))
      .collect().map(_.getString(0)) should contain theSameElementsAs Array("a", "b")

    // multi-column case class
    Seq(TestData("a", 7), TestData("b", 3), TestData("a", 2)).toDS
      .distinctRows(Seq('x), Seq('y.desc))
      .collect() should contain theSameElementsAs Array(TestData("a", 7), TestData("b", 3))
  }

  test("nvl replaces null") {
    Seq("a", "b", null).toDS.select(nvl('value, "c"))
      .as[String].collect should contain theSameElementsAs Array("a", "b", "c")

    Seq(("a", "a1"), (null, "b1")).toDF("a", "b").withColumn("a", nvl('a, 'b))
      .as[(String, String)].collect should contain theSameElementsAs Array(("a", "a1"), ("b1", "b1"))
  }

  test("can generate the schema of a case class") {
    val schema = Seq(TestData("a", 7), TestData("b", 3)).toDS.schema

    schemaOf[TestData] shouldEqual schema
  }

  test("to_date_str: numeric input, should give normal output") {
    Seq(("2019", "04", "10")).toDF("year", "month", "day").select(to_date_str('year, 'month, 'day))
      .as[String].collect should contain theSameElementsAs Array("2019-04-10")

    Seq((2019, 4, 10)).toDF("year", "month", "day").select(to_date_str('year, 'month, 'day))
      .as[String].collect should contain theSameElementsAs Array("2019-04-10")
  }

  test("to_date_str: text input, should return null") {
    Seq(("2019", "Four", "Ten")).toDF("year", "month", "day").select(to_date_str('year, 'month, 'day))
      .as[String].collect should contain theSameElementsAs Array(null)
  }

  test("to_date_str: null input, should return null") {
    Seq(("2019", "04", null)).toDF("year", "month", "day").select(to_date_str('year, 'month, 'day))
      .as[String].collect should contain theSameElementsAs Array(null)
  }

  test("can add a null string lit") {
    Seq("a", "b", "c").toDF("a").withColumn("b", null_string).select('b).distinct().as[String].collect should contain theSameElementsAs Array(null)
  }

  test("to_date_str: out of range input, should return null") {
    Seq(("2019", "13", "32")).toDF("year", "month", "day").select(to_date_str('year, 'month, 'day))
      .as[String].collect should contain theSameElementsAs Array(null)
  }

  test("withColumns to a DataFrame") {
    val inputDF = Seq((1, 2, 3), (2, 4, 8)).toDF("dfCol1", "dfCol2", "dfCol3")
    val resultDF = inputDF.withColumns(("dfCol1plus1", 'dfCol1 + 1), ("dfCol2x2", $"dfCol2" * 2))
    val expectedDF = Seq((1, 2, 3, 2, 4), (2, 4, 8, 3, 8)).toDF("dfCol1", "dfCol2", "dfCol3", "dfCol1plus1", "dfCol2x2")

    // test column names and values are as expected
    resultDF.columns should contain theSameElementsAs Array("dfCol1", "dfCol2", "dfCol3", "dfCol1plus1", "dfCol2x2")
    resultDF.collect should contain theSameElementsAs expectedDF.collect

    // with dataset as well
    Seq(TestData("a", 7), TestData("b", 3)).toDS()
      .withColumns("a" -> 'y * 10, "b" -> ('y + 100))
      .select('a, 'b).as[(Int, Int)].collect should contain theSameElementsAs Array((70, 107), (30, 103))
  }

  test("withColumnsRenamed within a DataFrame") {
    val inputDF = Seq((1, 2, 3), (2, 4, 8)).toDF("dfCol1", "dfCol2", "dfCol3")
    val resultDF = inputDF.withColumnsRenamed(Map("dfCol1" -> "col1", "dfCol2" -> "col2").toSeq: _*)
    val expectedDF = Seq((1, 2, 3), (2, 4, 8)).toDF("col11", "col2", "dfCol3")

    // test column names and values are as expected
    resultDF.columns should contain theSameElementsAs Array("col1", "col2", "dfCol3")
    resultDF.collect should contain theSameElementsAs expectedDF.collect

    // with dataset as well
    Seq(TestData("a", 7), TestData("b", 3)).toDS()
      .withColumnsRenamed("x" -> "a", "y" -> "b")
      .select('a, 'b).as[(String, Int)].collect should contain theSameElementsAs Array(("a", 7), ("b", 3))
  }

  test("load csv file into dataframe") {
    val raw = Seq(PartData("a", 7, 2019, 1, 10), PartData("b", 3, 2018, 2, 5))

    val csvData =
      """
        |x,y,year,month,day
        |a,7,2019,1,10
        |b,3,2018,2,5
      """.stripMargin
    FileUtils.deleteDirectory(new File("/tmp/footables/"))
    FileUtils.writeStringToFile(new File("/tmp/footables/test.csv"), csvData)
    spark.readCsv("/tmp/footables/test.csv").as[PartData].collect() should contain theSameElementsAs raw
    FileUtils.deleteDirectory(new File("/tmp/footables"))
  }

  test("count one column") {
    val inputDF = Seq(10, 20, 30, 20).toDF("dfCol1")

    val resultDF = inputDF.countBy('dfCol1) // test column input
    val resultDF1 = inputDF.countBy("dfCol1") // test string input

    val expectedDF = Array(Row(20, 2), Row(10, 1), Row(30, 1))

    resultDF.columns should contain theSameElementsAs Array("dfCol1", "count")
    resultDF.collect should contain theSameElementsAs expectedDF

    resultDF1.columns should contain theSameElementsAs Array("dfCol1", "count")
    resultDF1.collect should contain theSameElementsAs expectedDF
  }

  test("count columns") {
    val inputDF = Seq((10, 20, 30), (20, 30, 40), (20, 30, 40)).toDF("dfCol1", "dfCol2", "dfCol3")

    val resultDF = inputDF.countBy('dfCol1, 'dfCol2, 'dfCol3) //test column input
    val resultDF1 = inputDF.countBy("dfCol1", "dfCol2", "dfCol3") // test string input

    val expectedDF = Array(Row(20, 30, 40, 2), Row(10, 20, 30, 1))

    resultDF.columns should contain theSameElementsAs Array("dfCol1", "dfCol2", "dfCol3", "count")
    resultDF.collect should contain theSameElementsAs expectedDF

    resultDF1.columns should contain theSameElementsAs Array("dfCol1", "dfCol2", "dfCol3", "count")
    resultDF1.collect should contain theSameElementsAs expectedDF
  }

  test("count unique column") {
    val inputDF = Seq((10, 20), (20, 30)).toDF("dfCol1", "dfCol2")

    val resultDF = inputDF.countBy('dfCol1, 'dfCol2) //test column input
    val resultDF1 = inputDF.countBy("dfCol1", "dfCol2") // test string input

    val expectedDF = Seq(Row(10, 20, 1), Row(20, 30, 1))

    resultDF.columns should contain theSameElementsAs Array("dfCol1", "dfCol2", "count")
    resultDF.collect should contain theSameElementsAs expectedDF

    resultDF1.columns should contain theSameElementsAs Array("dfCol1", "dfCol2", "count")
    resultDF1.collect should contain theSameElementsAs expectedDF
  }

  test("converts timestamp to milliseconds") {
    Seq("2019-04-19 00:55:05.131", "2016-02-16 13:35:05.178").toDF("time")
      .withColumn("ts", 'time.cast("timestamp"))
      .withColumn("value", millis('ts))
      .select('value).as[Long].collect() should contain theSameElementsAs Array(1555635305131L, 1455629705178L)
  }

  test("countUnique returns unique number of values partitioned by a single column") {
    val data = Seq(
      (1, 1),
      (2, 1),
      (2, 2),
      (3, 1),
      (3, 1)
    ).toDF("x", "y")

    val result = data.withColumn("count", countUnique('y, 'x))
    val expected = Array(
      Row(1, 1, 1),
      Row(2, 1, 2),
      Row(2, 2, 2),
      Row(3, 1, 1),
      Row(3, 1, 1)
    )

    result.collect should contain theSameElementsAs expected
  }

  test("countUnique returns unique number of values partitioned by multiple column") {
    val data = Seq(
      (1, 1, 1),
      (1, 1, 1),
      (1, 1, 1),
      (2, 1, 1),
      (2, 1, 1),
      (2, 2, 1),
      (3, 1, 1),
      (3, 1, 2),
      (3, 2, 1)
    ).toDF("x", "y", "z")

    val result = data.withColumn("count", countUnique('z, 'x, 'y))
    val expected = Array(
      Row(1, 1, 1, 1),
      Row(1, 1, 1, 1),
      Row(1, 1, 1, 1),
      Row(2, 1, 1, 1),
      Row(2, 1, 1, 1),
      Row(2, 2, 1, 1),
      Row(3, 1, 1, 2),
      Row(3, 1, 2, 2),
      Row(3, 2, 1, 1)
    )

    result.collect should contain theSameElementsAs expected
  }

  test("countUnique returns unique number of values in entire column when no partitioning columns are specified") {
    val data = Seq(
      (1, 1),
      (2, 1),
      (2, 2),
      (3, 1),
      (3, 1)
    ).toDF("x", "y")

    val result = data.withColumn("count", countUnique('y))
    val expected = Array(
      Row(1, 1, 2),
      Row(2, 1, 2),
      Row(2, 2, 2),
      Row(3, 1, 2), Row(3, 1, 2)
    )

    result.collect should contain theSameElementsAs expected
  }

  test("cast function will convert types") {
    val data = Seq(("a1", 7, 6, 3, 9, 11.5), ("a2", 8, 7, 2, 1, 9.3)).toDF("x", "y", "year", "month", "day", "size").cache()
    data.cast[TestData].collect() should contain theSameElementsAs Array(TestData("a1", 7), TestData("a2", 8))
    data.cast[PartData].collect() should contain theSameElementsAs Array(PartData("a1", 7, 6, 3, 9), PartData("a2", 8, 7, 2, 1))
    data.unpersist()
  }

  test("cast function will throw exception when column has incorrect simple DataType, ignoring nullability") {
    the[IllegalArgumentException] thrownBy Seq((1, 3)).toDF("x", "y").cast[TestData].collect should have message
      """DataType for 'x' column doesn't match expected DataType in 'com.jarvis.utils.SparkUtilsTest.TestData' schema.
        |Expected: StringType
        |Received: IntegerType""".stripMargin

    //    root
    //    |-- x: string (nullable = true)
    //    |-- y: integer (nullable = false)
    noException should be thrownBy Seq(("1", 3)).toDF("x", "y").cast[TestData]

    //    root
    //    |-- x: string (nullable = true)
    //    |-- y: integer (nullable = true)
    noException should be thrownBy Seq(("1", 3.asInstanceOf[java.lang.Integer])).toDF("x", "y").cast[TestData]
  }

  test("cast function will throw exception when multiple columns have incorrect DataTypes") {
    the[IllegalArgumentException] thrownBy Seq((1, "3")).toDF("x", "y").cast[TestData].collect should have message
      """DataType for 'x' column doesn't match expected DataType in 'com.jarvis.utils.SparkUtilsTest.TestData' schema.
        |Expected: StringType
        |Received: IntegerType
        |DataType for 'y' column doesn't match expected DataType in 'com.jarvis.utils.SparkUtilsTest.TestData' schema.
        |Expected: IntegerType
        |Received: StringType""".stripMargin
  }

  test("cast function will throw exception when multiple columns have incorrect DataTypes, even in the case of duplicate columns") {
    the[IllegalArgumentException] thrownBy Seq((1, 1, "3")).toDF("x", "x", "y").cast[TestData].collect should have message
      """DataType for 'x' column doesn't match expected DataType in 'com.jarvis.utils.SparkUtilsTest.TestData' schema.
        |Expected: StringType
        |Received: IntegerType
        |DataType for 'y' column doesn't match expected DataType in 'com.jarvis.utils.SparkUtilsTest.TestData' schema.
        |Expected: IntegerType
        |Received: StringType""".stripMargin
  }

  test("cast function will throw exception when column has incorrect ArrayType, ignoring nullability") {
    the[IllegalArgumentException] thrownBy Seq(Seq("1")).toDF("x").cast[TestCastArrayTypeData].collect should have message
      """DataType for 'x' column doesn't match expected DataType in 'com.jarvis.utils.SparkUtilsTest.TestCastArrayTypeData' schema.
        |Expected: ArrayType(IntegerType,false)
        |Received: ArrayType(StringType,true)""".stripMargin

    //    root
    //    |-- x: array (nullable = true)
    //    |    |-- element: integer (containsNull = false)
    noException should be thrownBy Seq(Seq[Int](1)).toDF("x").cast[TestCastArrayTypeData].collect

    //    root
    //    |-- x: array (nullable = true)
    //    |    |-- element: integer (containsNull = true)
    noException should be thrownBy Seq(Seq[java.lang.Integer](1)).toDF("x").cast[TestCastArrayTypeData].collect
  }

  test("cast function will throw exception when column has incorrect MapType, ignoring nullability") {
    the[Exception] thrownBy Seq(Map("1" -> "1")).toDF("x").cast[TestCastMapTypeData].collect should have message
      """DataType for 'x' column doesn't match expected DataType in 'com.jarvis.utils.SparkUtilsTest.TestCastMapTypeData' schema.
        |Expected: MapType(IntegerType,IntegerType,false)
        |Received: MapType(StringType,StringType,true)""".stripMargin

    //    root
    //    |-- x: map (nullable = true)
    //    |    |-- key: integer
    //    |    |-- value: integer (valueContainsNull = false)
    noException should be thrownBy Seq(Map[Int, Int](1 -> 1)).toDF("x").cast[TestCastMapTypeData].collect
    //    root
    //    |-- x: map (nullable = true)
    //    |    |-- key: integer
    //    |    |-- value: integer (valueContainsNull = true)
    noException should be thrownBy Seq(Map(1.asInstanceOf[java.lang.Integer] -> 1.asInstanceOf[java.lang.Integer])).toDF("x").cast[TestCastMapTypeData].collect
    //    root
    //    |-- x: map (nullable = true)
    //    |    |-- key: integer
    //    |    |-- value: integer (valueContainsNull = false)
    noException should be thrownBy Seq(Map(1.asInstanceOf[java.lang.Integer] -> 1)).toDF("x").cast[TestCastMapTypeData].collect

    //    root
    //    |-- x: map (nullable = true)
    //    |    |-- key:s integer
    //    |    |-- value: integer (valueContainsNull = true)
    noException should be thrownBy Seq(Map(1 -> 1.asInstanceOf[java.lang.Integer])).toDF("x").cast[TestCastMapTypeData].collect
  }

  test("cast function will throw exception when column has incorrect StructType, ignoring nullability") {
    the[IllegalArgumentException] thrownBy spark.emptyDataFrame.withColumn("x", struct(lit(1) as "x", lit(1) as "y")).cast[TestCastStructTypeData].collect should have message
      """DataType for 'x' column doesn't match expected DataType in 'com.jarvis.utils.SparkUtilsTest.TestCastStructTypeData' schema.
        |Expected: StructType(StructField(x,StringType,true), StructField(y,IntegerType,false))
        |Received: StructType(StructField(x,IntegerType,false), StructField(y,IntegerType,false))""".stripMargin

    //    root
    //    |-- x: struct (nullable = false)
    //    |    |-- x: string (nullable = false)
    //    |    |-- y: integer (nullable = false)
    noException should be thrownBy spark.emptyDataFrame.withColumn("x", struct(lit("1") as "x", lit(1) as "y")).cast[TestCastStructTypeData].collect

    //    root
    //    |-- x: struct (nullable = true)
    //    |    |-- x: string (nullable = true)
    //    |    |-- y: integer (nullable = false)
    noException should be thrownBy Seq(Tuple1(TestData("1", 1))).toDF("x").cast[TestCastStructTypeData].collect

    //    root
    //    |-- x: struct (nullable = true)
    //    |    |-- x: string (nullable = true)
    //    |    |-- y: integer (nullable = true)
    noException should be thrownBy Seq(Tuple1(TestCastData("1", 1))).toDF("x").cast[TestCastStructTypeData].collect
  }

  test("can write to single CSV file") {
    val raw = Seq(PartData("a", 7, 2019, 1, 10), PartData("b", 3, 2018, 2, 5)).toDS()
    val fileLoc = "/tmp/spark-utils-csv.csv"
    val file = new java.io.File(fileLoc)
    FileUtils.deleteQuietly(file)

    try {
      raw.saveCsv(fileLoc)
      val text = FileUtils.readFileToString(file).split("\n")

      text should contain theSameElementsAs Array("x,y,year,month,day", "a,7,2019,1,10", "b,3,2018,2,5")

      // read back through readCsv function
      spark.readCsv(fileLoc).as[PartData].collect() should contain theSameElementsAs raw.collect()
    } finally {
      FileUtils.deleteQuietly(file)
    }
  }
}

object SparkUtilsTest {
  case class PartData(x: String, y: Int, year: Int, month: Int, day: Int)

  case class TestData(x: String, y: Int)

  case class TestCastData(x: String, y: java.lang.Integer)

  case class TestCastArrayTypeData(x: Seq[Int])

  case class TestCastMapTypeData(x: Map[Int, Int])

  case class TestCastStructTypeData(x: TestData)
}
