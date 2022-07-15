package com.jarvis.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

import java.sql._
import java.text.SimpleDateFormat

/** Filters a column by an inclusive range. Meant to be equivalent to ANSI SQL's BETWEEN operator */
case class DataRange[T: Ordering](private val column: String, private val start: T, private val end: T) {
  require(column != null && start != null && end != null, s"Null parameters are not allowed for DateRange.  Parameters provided are column: $column, start: $start, end: $end")
  require(implicitly[Ordering[T]].compare(start, end) < 1, s"parameter $start must <= $end")

  def date2String(d: java.util.Date) = new SimpleDateFormat("yyyy-MM-dd").format(d)

  val queryString: String = start match {
    case x: AnyRef if (x.isInstanceOf[String] || x.isInstanceOf[Timestamp]) && start == end => s"($column = '$start')"
    case x: AnyRef if (x.isInstanceOf[String] || x.isInstanceOf[Timestamp]) => s"($column BETWEEN '$start' and '$end')"
    case x: AnyRef if x.isInstanceOf[java.util.Date] && start == end => s"($column = '${date2String(start.asInstanceOf[java.util.Date])}')"
    case x: AnyRef if x.isInstanceOf[java.util.Date] => s"($column BETWEEN '${date2String(start.asInstanceOf[java.util.Date])}' and '${date2String(end.asInstanceOf[java.util.Date])}')"
    case _: java.lang.Number if start == end => s"($column = $start)"
    case _: java.lang.Number => s"($column BETWEEN $start and $end)"
  }
}

object DataRange {
  def apply[T: Ordering](column: String, start: T): DataRange[T] = new DataRange[T](column, start, start)

  implicit def dateRange2Column(d: DataRange[_]): Column = expr(d.queryString)

  // annoying, but we'll have to implement ordering for these to make them acceptable
  implicit def orderedDate: Ordering[Date] = new Ordering[Date] {
    override def compare(x: Date, y: Date): Int = x.compareTo(y)
  }

  implicit def orderedTimestamp: Ordering[Timestamp] = new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }
}