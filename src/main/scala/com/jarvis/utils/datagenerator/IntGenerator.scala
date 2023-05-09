package com.jarvis.utils.datagenerator

case class IntGenerator(min: Int = Integer.MIN_VALUE, max: Int = Integer.MAX_VALUE) extends Generator {
  require(max > min, "Maximum must exceed minimum for numeric types")
  override def generate: Int = (Math.random() * (max - min) + min).toInt
}
