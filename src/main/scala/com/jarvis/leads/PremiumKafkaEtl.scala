package com.jarvis.leads

import com.jarvis.leads.PremiumSchemas.{Lead, Premium}
import com.jarvis.leads.Utils._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object PremiumKafkaEtl extends SparkApp {
  def saveToMySql: (Dataset[Premium], Long) => Unit = (df: Dataset[Premium], batchId: Long) => {
    df.withColumn("batchId", lit(batchId))
      .write.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/demo?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
      .option("dbtable", "demo.premiums")
      .option("user", "root")
      .option("password", "root")
      .mode("append")
      .save()
  }

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val dataDir = "/mnt/c/Denton/work/de_eng_test/"

    val leadData = {
      spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "my-topic").load()
        .select('value.cast("string") as "json")
        .select(from_json('json, schemaOf[Lead]) as "data")
        .select("data.*").as[Lead]
    }

    // do transformations
    val prep = PremiumTransform.transform(leadData)

    // write every 3 seconds of data
    val query = prep.writeStream
      .option("checkpointLocation", dataDir+"check-premiums")
      .outputMode(OutputMode.Append)
      .trigger(Trigger.ProcessingTime(3000))
      .foreachBatch(saveToMySql)
      .start()

    query.awaitTermination()
    query.stop()
  }
}
