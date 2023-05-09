package com.jarvis.utils.datagenerator

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GeneratorTest extends AnyFunSuite with Matchers {
  test("explore generators") {
    "bob".gen.generate shouldBe("bob")

    val xInt = IntGenerator(1, 100).generate
    xInt should (be >= 1 and be <= 100)

  }

}
