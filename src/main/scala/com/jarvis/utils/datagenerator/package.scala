package com.jarvis.utils

package object datagenerator {
  implicit class AnyGen(a: Any) {
    def gen: Generator = a match {
      case _: Any => new Generator { // literal generator
        override def generate: Any = a
      }
    }
  }
}
