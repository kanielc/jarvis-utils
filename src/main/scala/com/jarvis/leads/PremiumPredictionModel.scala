package com.jarvis.leads

import com.jarvis.leads.Utils.SparkApp
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

object PremiumPredictionModel extends SparkApp {
  def main(args: Array[String]): Unit = {

    val prep = spark.table("lead_model_data").cache()
    val pipeline = createPipeline
    val model = pipeline.fit(prep)
    model.transform(prep).show()

    prep.unpersist()
  }

  private def createPipeline = {
    val assembler = new VectorAssembler()
      .setInputCols(
        Array("credit_score", "age", "marketing_rank", "gender", "urgency")
      )
      .setOutputCol("features")

    // not going for style
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setLabelCol("expected_premium")
      .setElasticNetParam(0.8)

    new Pipeline().setStages(Array(assembler, lr))
  }
}
