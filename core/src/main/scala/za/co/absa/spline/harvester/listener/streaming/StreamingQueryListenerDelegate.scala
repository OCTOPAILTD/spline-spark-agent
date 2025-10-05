/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.harvester.listener.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{FileStreamSink, IncrementalExecution, StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import za.co.absa.spline.agent.SplineAgent

import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration._


class StreamingQueryListenerDelegate(sparkSession: SparkSession, agent: SplineAgent) extends StreamingQueryListener {
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    // do nothing
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    // do nothing
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    if (event.progress.numInputRows > 0) {
      val incrementalExecution = processQuery(sparkSession.streams.get(event.progress.id))
      val duration = Duration(event.progress.durationMs.asScala.values.foldLeft(0L)(_ + _), TimeUnit.MILLISECONDS)

      agent.handle(incrementalExecution, Right(duration.toNanos.nanos))
    }
  }

  @tailrec
  private def processQuery(query: StreamingQuery): IncrementalExecution = {
    query match {
      case se: StreamExecution =>
        se.sink match {
          case fileStreamSink: FileStreamSink => FileStreamWriter.addWriteFileNode(se, fileStreamSink)
          case _ => se.lastExecution
        }
      case sw: StreamingQueryWrapper => processQuery(sw.streamingQuery)
    }
  }
}
