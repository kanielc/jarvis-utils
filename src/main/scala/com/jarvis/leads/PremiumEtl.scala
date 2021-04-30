package com.jarvis.leads

import com.jarvis.leads.PremiumSchemas.Lead
import com.jarvis.leads.Utils._
import org.apache.log4j.{Level, Logger}

object PremiumEtl extends SparkApp {
  def main(args: Array[String]): Unit = {

    import spark.implicits._

    Logger.getRootLogger.setLevel(Level.WARN)

    val leads = spark.read
      .schema(schemaOf[Lead])
      .option("multiLine", true)
      .option("mode", "PERMISSIVE")
      .json("C:\\Denton\\work\\de_eng_test\\lead*.json")
      .as[Lead]

    val prep = PremiumTransform.transform(leads)
    prep.write.mode("overwrite").saveAsTable("lead_model")

    //spark.read.table("lead_model").write.mode("overwrite").parquet("C:\\Denton\\work\\de_eng_test\\table.parquet")
    spark.read.table("lead_model").show(false)
  }
}
