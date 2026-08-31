/*
 * Copyright 2021 ABSA Group Limited
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

package za.co.absa.spline.harvester.dispatcher

import org.apache.commons.configuration.BaseConfiguration
import org.apache.commons.io.FileUtils.readFileToString
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.commons.io.TempDirectory
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.spline.producer.model._

import java.io.File
import java.util.UUID

class HDFSLineageDispatcherConfigSpec
  extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  // The HDFSLineageDispatcher companion object touches SparkContext.getOrCreate(),
  // so a (local) Spark session must exist before the dispatcher is constructed.
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit =
    spark = SparkSession.builder().master("local[1]").appName("hdfs-dispatcher-config-spec").getOrCreate()

  override protected def afterAll(): Unit =
    if (spark != null) spark.stop()

  behavior of "HDFSLineageDispatcher"

  it should "write lineage under the directory configured in the dispatcher config" in {
    val lineageDir = TempDirectory("spline_hdfs_cfg_", "", pathOnly = true).deleteOnExit()

    val dispatcher = new HDFSLineageDispatcher(new BaseConfiguration {
      addProperty("directory", lineageDir.toURI.toString)
      addProperty("fileName", "_LINEAGE")
      addProperty("fileBufferSize", 4096)
      addProperty("filePermissions", "777")
    })

    val planId = UUID.fromString("00000000-0000-0000-0000-000000000000")
    val plan = ExecutionPlan(
      id = Some(planId),
      name = "My Test Job",
      discriminator = None,
      labels = Map.empty,
      operations = Operations(
        write = WriteOperation("out", append = false, "op-0", "Write", Seq.empty, Map.empty, Map.empty),
        reads = Seq.empty,
        other = Seq.empty
      ),
      attributes = Seq.empty,
      expressions = Expressions(Seq.empty, Seq.empty),
      systemInfo = NameAndVersion("spark", "3.5"),
      agentInfo = NameAndVersion("spline", "2.3.0"),
      extraInfo = Map.empty
    )
    val event = ExecutionEvent(
      planId = planId,
      labels = Map.empty,
      timestamp = 1L,
      durationNs = None,
      discriminator = None,
      error = None,
      extra = Map("appId" -> "app-123")
    )

    dispatcher.send(plan)
    dispatcher.send(event)

    // <directory>/<sanitizedPlanName>/<runID>/lineage_<planId>.json
    val expectedFile = new File(new File(lineageDir.toURI), s"My_Test_Job/app-123/lineage_$planId.json")
    expectedFile.exists should be(true)
    expectedFile.length should be > 0L

    val lineageJson = readFileToString(expectedFile, "UTF-8").fromJson[Map[String, Map[String, _]]]
    lineageJson should contain key "executionPlan"
    lineageJson should contain key "executionEvent"
    lineageJson("executionPlan")("id") should equal(lineageJson("executionEvent")("planId"))
  }

  it should "prune older run folders for the same job (keep-last-run)" in {
    val lineageDir = TempDirectory("spline_hdfs_keeplast_", "", pathOnly = true).deleteOnExit()

    def dispatcher = new HDFSLineageDispatcher(new BaseConfiguration {
      addProperty("directory", lineageDir.toURI.toString)
      addProperty("fileName", "_LINEAGE")
      addProperty("fileBufferSize", 4096)
      addProperty("filePermissions", "777")
    })

    val planId = UUID.fromString("11111111-1111-1111-1111-111111111111")
    def plan = ExecutionPlan(
      id = Some(planId),
      name = "Job",
      discriminator = None,
      labels = Map.empty,
      operations = Operations(
        write = WriteOperation("out", append = false, "op-0", "Write", Seq.empty, Map.empty, Map.empty),
        reads = Seq.empty,
        other = Seq.empty
      ),
      attributes = Seq.empty,
      expressions = Expressions(Seq.empty, Seq.empty),
      systemInfo = NameAndVersion("spark", "3.5"),
      agentInfo = NameAndVersion("spline", "2.3.0"),
      extraInfo = Map.empty
    )
    def event(runId: String) = ExecutionEvent(planId, Map.empty, 1L, None, None, None, Map("appId" -> runId))

    val d1 = dispatcher; d1.send(plan); d1.send(event("run-1"))
    val d2 = dispatcher; d2.send(plan); d2.send(event("run-2"))

    val jobDir = new File(new File(lineageDir.toURI), "Job")
    val runFolders = Option(jobDir.listFiles).toSeq.flatten.filter(_.isDirectory).map(_.getName).toSet
    runFolders should be(Set("run-2"))
  }

}
