package com.jarvis.leads

import com.jarvis.leads.PremiumSchemas.Lead
import com.jarvis.leads.Utils._

object PremiumEtl extends SparkApp {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    import org.apache.spark.sql.streaming.Trigger

    val dataDir = "C:\\Denton\\work\\de_eng_test\\"
    val checkPoint = dataDir + "checkpoint"
    val leadData = {
      spark.readStream.format("json").schema(schemaOf[Lead])
        .option("multiLine", true)
        .option("mode", "PERMISSIVE")
        .option("checkpointLocation", checkPoint)
        .load(dataDir + "gen*.json")
        .as[Lead]
    }

    val prep = PremiumTransform.transform(leadData)
    prep.writeStream.option("checkpointLocation", checkPoint).trigger(Trigger.Once).format("parquet").start(dataDir + "table.parquet")

    spark.read.parquet(dataDir + "table.parquet").show(false)
  }
}
