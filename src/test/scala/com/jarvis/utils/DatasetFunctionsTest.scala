package com.jarvis.utils

import SparkUtils._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object DatasetFunctionsTest {

  case class TestData(x: String, y: Int)
  case class Person(name: String, age: Int, income: java.lang.Double)

}

class DatasetFunctionsTest extends AnyFunSuite with Matchers {
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
  import DatasetFunctionsTest._

  test("basic") {
    val ds = Seq((1, 2), (1, 1)).toDF("a", "b")
    ds.distinctRows(Seq("a"), Seq('b)).collect() should contain theSameElementsAs Array(Row(1, 1))
  }

  test("respects null first") {
    val ds = Seq[(Int, Integer)]((1, 2), (1, 1), (1, null)).toDF("a", "b")
    ds.distinctRows(Seq("a"), Seq('b)).collect() should contain theSameElementsAs Array(Row(1, null))
  }

  test("respects sort direction") {
    val ds = Seq[(Int, Integer)]((1, 2), (1, 1), (1, null), (2, 5), (2, 3)).toDF("a", "b")
    ds.distinctRows(Seq("a"), Seq('b.desc_nulls_last)).collect() should contain theSameElementsAs Array(Row(1, 2), Row(2, 5))
  }

  test("complex sorting") {
    val ds = Seq((1, "foo"), (1, "bar"), (1, "gaz")).toDF("a", "b")
    ds.distinctRows(Seq("a"), Seq('b.substr(1, 1))).collect() should contain theSameElementsAs Array(Row(1, "bar"))
  }

  test("distinct by removes duplicates") {
    // with a dataset
    Seq("a", "b", "a").toDF("value").distinctRows(Seq("value"), Seq(lit(true)))
      .as[String].collect should contain theSameElementsAs Array("a", "b")

    // with a dataframe
    Seq("a", "b", "a").toDF("value").distinctRows(Seq("value"), Seq(lit(true)))
      .collect().map(_.getString(0)) should contain theSameElementsAs Array("a", "b")

    // multi-column case class
    Seq(TestData("a", 7), TestData("b", 3), TestData("a", 2)).toDS()
      .distinctRows(Seq("x"), Seq('y.desc))
      .collect() should contain theSameElementsAs Array(TestData("a", 7), TestData("b", 3))
  }

  test("adopt works") {
    val df = Seq(("Bob", 5), ("Sally", 10)).toDF("name", "age")
    df.adopt[Person].collect() should contain theSameElementsAs Array(
      Person("Bob", 5, null),
      Person("Sally", 10, null)
    )

    an[RuntimeException] should be thrownBy Seq("Bob").toDF("name").adopt[Person].collect() // because age is a required field
  }
}


