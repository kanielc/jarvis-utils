package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

object JarvisInternal {
  def toDataframe(rdd: RDD[InternalRow], spark: SparkSession, schema: StructType, isStreaming: Boolean = false): DataFrame = {
    spark.internalCreateDataFrame(rdd, schema, isStreaming)
  }
}
