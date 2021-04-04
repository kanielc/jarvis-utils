package com.jarvis.utils

import org.apache.spark.sql.functions.{current_date, date_add}

import java.time.ZoneOffset

class DateTimeUtilsTest extends SparkTest {
  import DateTimeUtils._
  import spark.implicits._

  test("add days to a given date") {
    plusDays("2018-01-10", 5) shouldEqual "2018-01-15"
    plusDays("2018-01-10", -5) shouldEqual "2018-01-05"
    plusDays("2018-01-10", 0) shouldEqual "2018-01-10"
    an[NullPointerException] should be thrownBy plusDays(null, 5)
  }

  test("return a list of dates between two given dates") {
    dateRange("2018-01-10", "2018-01-10") shouldEqual Seq("2018-01-10")
    dateRange("2018-01-10", "2018-01-14") shouldEqual Seq("2018-01-10", "2018-01-11", "2018-01-12", "2018-01-13", "2018-01-14")
    dateRange("2018-01-30", "2018-02-04") shouldEqual Seq("2018-01-30", "2018-01-31", "2018-02-01", "2018-02-02", "2018-02-03", "2018-02-04")
    dateRange("2018-12-25", "2019-01-05") shouldEqual Seq("2018-12-25", "2018-12-26", "2018-12-27", "2018-12-28", "2018-12-29", "2018-12-30", "2018-12-31", "2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04", "2019-01-05")
    an[IllegalArgumentException] should be thrownBy dateRange("2018-01-14", "2018-01-10")
  }

  test("return today's date") {
    Seq(1).toDS.select(current_date().cast("string")).as[String].collect.head shouldEqual today
  }

  test("return yesterday's date") {
    Seq(1).toDS().select(date_add(current_date(), -1).cast("string"))
      .as[String].collect.head shouldEqual yesterday
  }

  test("now UTC is close to currentTimeMillis") {
    val currentTime = System.currentTimeMillis()
    val now = nowUTC.toInstant(ZoneOffset.UTC).toEpochMilli

    now - currentTime should be < 2000
  }

  test("start of month") {
    startOfMonth("2021-02-19") shouldBe "2021-02-01"
    startOfMonth("2020-11-03") shouldBe "2020-11-01"
  }
}
