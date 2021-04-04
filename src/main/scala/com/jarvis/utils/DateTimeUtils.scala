package com.jarvis.utils

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

/* Several of these taken directly or with modifications from:
 https://github.com/kanielc/spark-utils/blob/master/src/main/scala/com/jakainas/functions/package.scala */
object DateTimeUtils {
  private val utcZoneId: ZoneId = ZoneId.of("UTC")

  /**
   * Adds days to a given date
   *
   * @param date    - date to be added to in the format of '2019-01-20'
   * @param numDays - number of days to add, can be negative
   * @return numDays after (or before if negative) `date`
   */
  def plusDays(date: String, numDays: Int): String = {
    parseDate(date).minusDays(-numDays).toString
  }

  /**
   * Converts a date string to a LocalDate
   *
   * @param date - date string to convert in the format of '2019-01-20'
   * @return LocalDate representation of the given date
   */
  def parseDate(date: String): LocalDate = {
    LocalDate.parse(date, DateTimeFormatter.ISO_DATE)
  }

  /**
   * Returns a list of dates that lie between two given dates
   *
   * @param start - start date (yyyy-mm-dd)
   * @param end   - end date (yyyy-mm-dd)
   * @return The dates between start and end in the form of a sequence of strings
   */
  def dateRange(start: String, end: String): IndexedSeq[String] = {
    val days = ChronoUnit.DAYS.between(parseDate(start), parseDate(end)).toInt
    require(days >= 0, s"Start date ($start) must be before end date ($end)!")
    (0 to days).map(d => plusDays(start, d))
  }

  /**
   * Today as a string in UTC
   *
   * @return Today's date in UTC(String)
   */
  def today: String = LocalDate.now(utcZoneId).toString

  /**
   * Yesterday as a string in UTC
   *
   * @return Yesterday's date in UTC(String)
   */
  def yesterday: String = LocalDate.now(utcZoneId).minusDays(1).toString

  /** Current time in UTC */
  def nowUTC: LocalDateTime = LocalDateTime.now(utcZoneId)

  /**
   * Presents the milliseconds at the start of the given date
   * @param date - date to get milliseconds for (YYYY-MM-DD)
   * @return - milliseconds at the start of the given date
   */
  def startOfDayMillis(date: String): Long = parseDate(date).atStartOfDay.toInstant(ZoneOffset.UTC).toEpochMilli

  /** Quick way to get the start of a month for a given date
   * @param date - date to get start for month (YYYY-MM-DD)
   * @return start of the given month
   */
  def startOfMonth(date: String): String = s"${date.substring(0, 7)}-01"

  /**
   * Convert a given number of milliseconds since Epoch to a date string
   * @param millis - milliseconds since epoch
   * @return - a UTC date in the format YYYY-MM-DD to which that millisecond count corresponds
   */
  def millisToDateUTC(millis: Long): String = Instant.ofEpochMilli(millis).atZone(utcZoneId).toLocalDate.toString

}
