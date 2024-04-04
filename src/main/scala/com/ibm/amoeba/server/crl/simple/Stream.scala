package com.ibm.amoeba.server.crl.simple

import java.util.UUID

class Stream(val streamId: StreamId,
             val streamWriter: StreamWriter,
             private var currentUUID: UUID,
             initialNextEntryOffset: Long):

  private var nextWriteOffset: Long = initialNextEntryOffset

  def recycleStream(): Unit =
    nextWriteOffset = 0
    currentUUID = UUID.randomUUID()


  def canWriteEntry(entry: LogEntry): Boolean = nextWriteOffset + entry.entrySize <= streamWriter.maxSizeInBytes


  // Writes entry to the stream and returns its location
  def writeEntry(entry: LogEntry): StreamLocation =
    val entryLocation = StreamLocation(streamId, nextWriteOffset, entry.entrySize.toInt)
    val entrySize = entry.entrySize

    val buffers = entry.createEntryBuffers(nextWriteOffset, currentUUID, streamId)

    streamWriter.write(streamId, nextWriteOffset, buffers)

    nextWriteOffset += entrySize
    entryLocation