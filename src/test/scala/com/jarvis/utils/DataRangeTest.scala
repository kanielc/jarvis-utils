package com.jarvis.utils

class DataRangeTest extends SparkTest {
  test("datarange works for int") {
    // This will also test the apply method
    DataRange("foo", 7).queryString shouldEqual "(foo = 7)"

    DataRange("foo", 7, 9).queryString shouldEqual "(foo BETWEEN 7 and 9)"
  }

}
