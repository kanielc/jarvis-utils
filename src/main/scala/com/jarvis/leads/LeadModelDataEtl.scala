package com.jarvis.leads

import com.jarvis.leads.LeadModelSchemas.Lead
import com.jarvis.leads.Utils._
import org.apache.log4j.{Level, Logger}

object LeadModelDataEtl extends SparkApp {
  def main(args: Array[String]): Unit = {

    import spark.implicits._

    Logger.getRootLogger.setLevel(Level.WARN)

    val leads = spark.read.schema(schemaOf[Lead]).option("multiLine", true)
      .option("mode", "PERMISSIVE").json("C:\\Denton\\work\\de_eng_test\\lead*.json").as[Lead]

    val prep = LeadModelDataTransform.transform(leads)
    prep.write.mode("overwrite").saveAsTable("lead_model")

    spark.read.table("lead_model").show(false)
  }
}


