/*
 * Copyright 2026 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.harvester.postprocessing.metadata

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SafePathEvaluatorSpec extends AnyFlatSpec with Matchers {

  private val sparkSession = SparkSession.builder
    .master("local")
    .appName("safePathTest")
    .config("k", "nice")
    .getOrCreate()

  private val bindings = Map("session" -> sparkSession)

  it should "evaluate allowed method chains" in {
    SafePathEvaluator.eval("session.conf().get('k')", bindings) shouldBe "nice"
    SafePathEvaluator.eval("session.sparkContext().appName()", bindings) shouldBe "safePathTest"
    SafePathEvaluator.eval("session.conf().get('spark.app.name')", bindings) shouldBe "safePathTest"
  }

  it should "reject disallowed method names" in {
    val ex = intercept[IllegalArgumentException] {
      SafePathEvaluator.eval("session.getClass()", bindings)
    }
    ex.getMessage should include("not allowed")
  }
}
