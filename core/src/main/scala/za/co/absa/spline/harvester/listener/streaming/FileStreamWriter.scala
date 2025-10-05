///*
// * Copyright 2019 ABSA Group Limited
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package za.co.absa.spline.harvester.listener.streaming
//
//import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2
//import org.apache.spark.sql.execution.streaming.{FileStreamSink, IncrementalExecution, StreamExecution}
//import org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter
//import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
//import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
//
///*
//  Currently SPARK doesn't create a logical streaming writer node in logical plan.
//  Spline creates custom FileStreamWriter as a temporary solutions
//  For more info visit: https://issues.apache.org/jira/browse/SPARK-27484
// */
//class FileStreamWriter(val fileStreamSink: FileStreamSink) extends StreamWriter {
//  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
//    // do nothing
//  }
//
//  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
//    // do nothing
//  }
//
//  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
//    throw new UnsupportedOperationException("FileStreamWriter.createWriterFactory()")
//  }
//}
//
//object FileStreamWriter {
//  def addWriteFileNode(se: StreamExecution, fileStreamSink: FileStreamSink): IncrementalExecution = {
//    val writeFileNode = WriteToDataSourceV2(
//      new MicroBatchWriter(se.lastExecution.currentBatchId, new FileStreamWriter(fileStreamSink)),
//      se.lastExecution.analyzed
//    )
//    new IncrementalExecution(
//      se.lastExecution.sparkSession,
//      writeFileNode,
//      se.lastExecution.outputMode,
//      se.lastExecution.checkpointLocation,
//      se.lastExecution.runId,
//      se.lastExecution.currentBatchId,
//      se.lastExecution.offsetSeqMetadata
//    )
//  }
//}
