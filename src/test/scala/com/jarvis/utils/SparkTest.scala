package com.jarvis.utils

import java.util.TimeZone

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

trait SparkTest extends AnyFunSuite with Matchers {
  val spark = SparkSession.builder().appName("test")
    .master("local[*]").enableHiveSupport().getOrCreate()

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  spark.sparkContext.setLogLevel("WARN")
}
