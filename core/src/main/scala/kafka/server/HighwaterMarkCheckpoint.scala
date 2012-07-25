/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.utils.Logging
import java.util.concurrent.locks.ReentrantLock
import java.io._

/**
 * This class handles the read/write to the highwaterMark checkpoint file. The file stores the high watermark value for
 * all topics and partitions that this broker hosts. The format of this file is as follows -
 * version
 * number of entries
 * topic partition highwatermark
 */

object HighwaterMarkCheckpoint {
  val highWatermarkFileName = ".highwatermark"
  val currentHighwaterMarkFileVersion = 0
}

class HighwaterMarkCheckpoint(val path: String) extends Logging {
  /* create the highwatermark file handle for all partitions */
  val name = path + "/" + HighwaterMarkCheckpoint.highWatermarkFileName
  private val hwFile = new File(name)
  private val hwFileLock = new ReentrantLock()
  // recover from previous tmp file, if required

  def write(highwaterMarksPerPartition: Map[(String, Int), Long]) {
    hwFileLock.lock()
    try {
      // write to temp file and then swap with the highwatermark file
      val tempHwFile = new File(hwFile + ".tmp")
      // it is an error for this file to be present. It could mean that the previous rename operation failed
      if(tempHwFile.exists()) {
        fatal("Temporary high watermark %s file exists. This could mean that the ".format(tempHwFile.getAbsolutePath) +
          "previous high watermark checkpoint operation has failed.")
        System.exit(1)
      }
      val hwFileWriter = new BufferedWriter(new FileWriter(tempHwFile))
      // checkpoint highwatermark for all partitions
      // write the current version
      hwFileWriter.write(HighwaterMarkCheckpoint.currentHighwaterMarkFileVersion.toString)
      hwFileWriter.newLine()
      // write the number of entries in the highwatermark file
      hwFileWriter.write(highwaterMarksPerPartition.size.toString)
      hwFileWriter.newLine()

      highwaterMarksPerPartition.foreach { partitionAndHw =>
        val topic = partitionAndHw._1._1
        val partitionId = partitionAndHw._1._2
        hwFileWriter.write("%s %s %s".format(topic, partitionId, partitionAndHw._2))
        hwFileWriter.newLine()
      }
      hwFileWriter.flush()
      hwFileWriter.close()
      // swap new high watermark file with previous one
      hwFile.delete()
      if(!tempHwFile.renameTo(hwFile)) {
        fatal("Attempt to swap the new high watermark file with the old one failed")
        System.exit(1)
      }
    }finally {
      hwFileLock.unlock()
    }
  }

  def read(topic: String, partition: Int): Long = {
    hwFileLock.lock()
    try {
      hwFile.length() match {
        case 0 => warn("No previously checkpointed highwatermark value found for topic %s ".format(topic) +
          "partition %d. Returning 0 as the highwatermark".format(partition))
        0L
        case _ =>
          val hwFileReader = new BufferedReader(new FileReader(hwFile))
          val version = hwFileReader.readLine().toShort
          version match {
            case HighwaterMarkCheckpoint.currentHighwaterMarkFileVersion =>
              val numberOfHighWatermarks = hwFileReader.readLine().toInt
              val partitionHighWatermarks =
                for(i <- 0 until numberOfHighWatermarks) yield {
                  val nextHwEntry = hwFileReader.readLine()
                  val partitionHwInfo = nextHwEntry.split(" ")
                  val highwaterMark = partitionHwInfo.last.toLong
                  val partitionId = partitionHwInfo.takeRight(2).head
                  // find the index of partition
                  val partitionIndex = nextHwEntry.indexOf(partitionId)
                  val topic = nextHwEntry.substring(0, partitionIndex-1)
                  ((topic, partitionId.toInt) -> highwaterMark)
                }
              hwFileReader.close()
              val hwOpt = partitionHighWatermarks.toMap.get((topic, partition))
              hwOpt match {
                case Some(hw) => debug("Read hw %d for topic %s partition %d from highwatermark checkpoint file"
                                        .format(hw, topic, partition))
                  hw
                case None => warn("No previously checkpointed highwatermark value found for topic %s ".format(topic) +
                  "partition %d. Returning 0 as the highwatermark".format(partition))
                0L
              }
            case _ => fatal("Unrecognized version of the highwatermark checkpoint file " + version)
            System.exit(1)
            -1L
          }
      }
    }finally {
      hwFileLock.unlock()
    }
  }
}