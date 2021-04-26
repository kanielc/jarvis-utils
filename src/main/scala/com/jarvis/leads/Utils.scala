package com.jarvis.leads

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}

import scala.reflect.runtime.universe._

object Utils {
  def schemaOf[T: TypeTag]: StructType = {
    val dataType = ScalaReflection.schemaFor[T].dataType
    dataType.asInstanceOf[StructType]
  }

  trait SparkApp {
    System.setProperty("hadoop.home.dir", "C:\\Denton\\Hadoop")
    val spark = {
      SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .enableHiveSupport()
        .getOrCreate()
    }
  }

  implicit class DSFunctions[T](val ds: Dataset[T]) extends AnyVal {
    def adopt[U: Encoder : TypeTag](implicit tp: TypeTag[T]): Dataset[U] = {
      val destSchema = schemaOf[U]
      val currSchema = ds.schema

      val resultColumns = destSchema.map { destField =>
        val lowerName = destField.name.toLowerCase
        val matching = currSchema.find(f => f.name.toLowerCase == lowerName && (f.dataType == destField.dataType || destField.dataType.isInstanceOf[StringType] || (destField.dataType.isInstanceOf[DecimalType] && f.dataType.isInstanceOf[DecimalType])))

        matching.fold(lit(null).cast(destField.dataType) as destField.name) { f => if (f.dataType == destField.dataType) ds(f.name) as destField.name else ds(f.name).cast(destField.dataType) as destField.name }
      }

      ds.select(resultColumns:_*).as[U]
    }
  }
}
