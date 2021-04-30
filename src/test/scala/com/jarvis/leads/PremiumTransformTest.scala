package com.jarvis.leads

import com.jarvis.leads.PremiumSchemas.Lead
import com.jarvis.utils.SparkTest
import Utils._
import com.jarvis.leads.PremiumTransformTest.Person

class PremiumTransformTest extends SparkTest {
  import spark.implicits._

  test("empty runs") {
    val ds = spark.emptyDataset[Lead]
    val res = PremiumTransform.transform(ds)

    res.count shouldEqual 0
  }

  test("adopt works") {
    val df = Seq(("Bob", 5), ("Sally", 10)).toDF("name", "age")
    df.adopt[Person].collect() should contain theSameElementsAs Array(
      Person("Bob", 5, null),
      Person("Sally", 10, null)
    )

    an[RuntimeException] should be thrownBy Seq("Bob")
      .toDF("name")
      .adopt[Person]
      .collect() // because age is a required field
  }

  // R3.1 - adfafsdouer
  test("gender handles word value and defaults") {
    val df = Seq("female", "male", "M", "F", "cat", null)
      .toDF("gender")
      .adopt[Lead] // female and male should take first letter, cat and null should end up as the default, which is female

    PremiumTransform
      .transform(df)
      .select('gender)
      .as[Int]
      .collect() should contain theSameElementsAs Array(0, 1, 1, 0, 0,
      0) // 1 - male, 0 - female
  }

  test("urgency cleanup and mapping") {
    // Not Sure - 3, Within 2 Months - 2, Immediately - 1
    val df = Seq(
      "Unsure",
      "Not sure",
      "Unknown",
      "We're good",
      "immediately",
      "WiTHin 2 Months"
    ).toDF("urgency").adopt[Lead]

    PremiumTransform
      .transform(df)
      .select('urgency)
      .as[java.lang.Integer]
      .collect() should contain theSameElementsAs Array(3, 3, 3, 3, 1, 2)
    PremiumTransform
      .transform(df)
      .select('urgency_immediately, 'urgency_within_2_months, 'urgency_not_sure)
      .as[(Int, Int, Int)]
      .collect() should contain theSameElementsAs Array(
      (0, 0, 1),
      (0, 0, 1),
      (0, 0, 1),
      (0, 0, 1),
      (1, 0, 0),
      (0, 1, 0)
    )
  }

  test("rough test of expected premium") {
    val ds = Seq(Lead(11, 3, "M", 43, 635, "Immediately")).toDS()
    PremiumTransform
      .transform(ds)
      .select('expected_premium.cast("decimal(5, 2)").cast("string"))
      .as[String]
      .collect() should contain theSameElementsAs Array(
      "118.76"
    ) // after rounding
  }
}

object PremiumTransformTest {
  case class Person(name: String, age: Int, income: java.lang.Double)
}
