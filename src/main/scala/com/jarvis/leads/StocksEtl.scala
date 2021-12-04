package com.jarvis.leads

import com.jarvis.leads.Utils._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object StocksEtl extends SparkApp {
  case class Stock(Date: String, Open: Double, High: Double, Low: Double, Close: Double, `Adj Close`: Double, Volume: Int)

  def saveToMySql: (Dataset[Stock], Long) => Unit = (df: Dataset[Stock], batchId: Long) => {
    df.withColumn("batchId", lit(batchId))
      .write.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/demo?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
      .option("dbtable", "demo.nbc")
      .option("user", "root")
      .option("password", "root")
      .mode("append")
      .save()
  }

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val dataDir = "/mnt/c/Denton/work/de_eng_test/"

    val stocks = {
      spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "nbc").load()
        .select('value.cast("string") as "json")
        .select(from_json('json, schemaOf[Stock]) as "data")
        .select("data.*")
        .as[Stock]
    }

    // write every 3 seconds of data
    val query = stocks.writeStream
      .option("checkpointLocation", dataDir+"check-stocks")
      .outputMode(OutputMode.Append)
      .trigger(Trigger.ProcessingTime(3000))
      .foreachBatch(saveToMySql)
      .start()

    query.awaitTermination()
    query.stop()
  }
}
