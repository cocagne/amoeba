package com.ibm.amoeba.server.crl.sweeper

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import java.util.UUID

import com.ibm.amoeba.common.DataBuffer

class LogFile(path: Path, val fileId: FileId, val maxSize: Long) {

  private val channel = FileChannel.open(path,
    StandardOpenOption.CREATE,
    StandardOpenOption.DSYNC,
    StandardOpenOption.READ,
    StandardOpenOption.WRITE)

  if (size < 16) {
    resetFile()
  }

  channel.position(channel.size)

  var fileUUID: UUID = readUUID(0)

  def size: Long = channel.size()

  def write(buffers: Array[ByteBuffer]): Unit = {
    channel.write(buffers)
  }

  def read(offset: Long, nbytes: Int): ByteBuffer = {
    if (offset + nbytes > size)
      throw new CorruptedEntry(s"Invalid File Location. Exceeds File Size offset $offset, nbytes $nbytes")
    val arr = new Array[Byte](nbytes)
    val bb = ByteBuffer.wrap(arr)
    channel.read(bb, offset)
    DataBuffer(arr)
  }

  def read(offset: Long, arr: Array[Byte]): Unit = {
    if (offset + arr.length > size)
      throw new CorruptedEntry(s"Invalid File Location. Exceeds File Size offset $offset, nbytes ${arr.length}")
    val bb = ByteBuffer.wrap(arr)
    channel.read(bb, offset)
  }

  private def readUUID(pos: Long): UUID = {
    val bb = ByteBuffer.allocate(16)
    channel.read(bb, pos)
    bb.position(0)
    val msb = bb.getLong()
    val lsb = bb.getLong()
    new java.util.UUID(msb, lsb)
  }

  def resetFile(): Unit = {
    channel.truncate(0)
    val uuid = java.util.UUID.randomUUID()
    val bb = ByteBuffer.allocate(16)
    bb.putLong(uuid.getMostSignificantBits)
    bb.putLong(uuid.getLeastSignificantBits)
    bb.position(0)
    channel.write(bb)
    fileUUID = uuid
  }

  def findLastValidEntry(): Option[(LogEntrySerialNumber, Long)] = {
    var offset = channel.size() - (channel.size() % 4096)
    var last: Option[(LogEntrySerialNumber, Long)] = None

    while (offset >= 4096 && last.isEmpty) {
      val testUUID = readUUID(offset - 16)

      if (testUUID == fileUUID) {
        val entryOffset = offset - Entry.StaticEntryFooterSize
        val bb = ByteBuffer.allocate(8)
        channel.read(bb, entryOffset)
        bb.position(0)
        val serial = bb.getLong()
        last = Some(new LogEntrySerialNumber(serial), entryOffset)
      }

      if (last.isEmpty) {
        offset -= 4096
      }
    }

    last
  }
}
