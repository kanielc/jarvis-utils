package com.jarvis.leads

import com.jarvis.leads.PremiumSchemas._
import com.jarvis.leads.Utils.DSFunctions
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object PremiumTransform {
  def transform(leads: Dataset[Lead]): Dataset[Premium] = {
    import leads.sparkSession.implicits._

    leads
      .withColumn("gender", upper('gender).substr(1, 1))
      .withColumn("gender", when(!'gender.isin("M", "F"), "F").otherwise('gender))
      .withColumn("gender", when('gender === "M", 1).otherwise(0)) // so gender = isMale
      .withColumn("urgency", lower('urgency))
      .withColumn("urgency", when(!'urgency.isin("within 2 months", "immediately", "not sure"), "not sure").otherwise('urgency))
      .withColumn("urgency_immediately", when('urgency === "immediately", 1).otherwise(0))
      .withColumn("urgency_within_2_months", when('urgency === "within 2 months", 1).otherwise(0))
      .withColumn("urgency_not_sure", when('urgency === "not sure", 1).otherwise(0))
      .withColumn("urgency", when('urgency === "immediately", 1)
        .when('urgency === "within 2 months", 2).when('urgency === "not sure", 3))
      .withColumn("expected_premium", exp(('credit_score * 0.001222) + ('age * 0.00927) - ('marketing_rank * 0.03404)
        + ('urgency_immediately * 0.031735) + ('urgency_within_2_months * 0.07091) + ('urgency_not_sure * 0.0233) + ('gender * 0.0509) + 3.622))
      .cast[Premium]
  }
}
