package com.jarvis.utils

import java.sql.Timestamp
import java.lang.IllegalArgumentException
import DataRange._

import java.text.SimpleDateFormat

class DataRangeTest extends SparkTest {
  test("datarange works for int") {
    // This will also test the apply method
    DataRange("foo", 7).queryString shouldEqual "(foo = 7)"

    DataRange("foo", 7, 9).queryString shouldEqual "(foo BETWEEN 7 and 9)"
  }

  test("datarange works for string date") {
    // This will also test the apply method
    DataRange("foo", "2020-01-02").queryString shouldEqual "(foo = '2020-01-02')"

    DataRange("foo", "2020-01-02", "2020-12-31").queryString shouldEqual "(foo BETWEEN '2020-01-02' and '2020-12-31')"
  }

  test("datarange works for date objects") {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val start = df.parse("2020-01-02")
    val end = df.parse("2020-12-31")
    DataRange("foo", start).queryString shouldEqual "(foo = '2020-01-02')"

    DataRange("foo", start, end).queryString shouldEqual "(foo BETWEEN '2020-01-02' and '2020-12-31')"
  }

  test("datarange works for timestamps") {
    val start = new Timestamp(1577923200000L)
    val end = new Timestamp(1609376117000L)
    DataRange("foo", start).queryString shouldEqual "(foo = '2020-01-02 00:00:00.0')"

    DataRange("foo", start, end).queryString shouldEqual "(foo BETWEEN '2020-01-02 00:00:00.0' and '2020-12-31 00:55:17.0')"
  }

  test("datarange null tests") {
    an [IllegalArgumentException] should be thrownBy DataRange(null, 7)
    an [IllegalArgumentException] should be thrownBy DataRange("foo", null)
    an [IllegalArgumentException] should be thrownBy DataRange("foo", new Integer(7), null)
  }

  test("datarange fails if ordering is wrong") {
    an [IllegalArgumentException] should be thrownBy DataRange("foo", 9, 7)
  }


}
