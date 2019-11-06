package com.ibm.amoeba.server.crl.sweeper

import java.nio.ByteBuffer
import java.util.UUID

private sealed abstract class Index {
  val index: Int
}
private object Zero extends Index {
  val index: Int = 0
}
private object One extends Index {
  val index: Int = 1
}
private object Two extends Index {
  val index: Int = 2
}


class TriFileStream(maxFileSize: Long, file0: LogFile, file1: LogFile, file2: LogFile) {

  val maxSize: Long = file0.maxSize

  val files = new Array[LogFile](3)
  files(0) = file0
  files(1) = file1
  files(2) = file2

  private var active: Index = Zero

  {
    var highest = files(0).findLastValidEntry() match {
      case None => LogEntrySerialNumber(0)
      case Some(n) => n._1
    }
    highest = files(1).findLastValidEntry() match {
      case None => highest
      case Some(n) => if (n._1.number > highest.number) {
          active = One
          n._1
        }
        else
          highest
    }
    highest = files(2).findLastValidEntry() match {
      case None => highest
      case Some(n) => if (n._1.number > highest.number) {
        active = Two
        n._1
      }
      else
        highest
    }
  }

  val entry: Entry = new Entry(maxFileSize, activeFileSize())

  def activeFileSize(): Long = {
    files(active.index).size
  }

  def status(): (FileId, UUID) = {
    val f = files(active.index)
    (f.fileId, f.fileUUID)
  }

  /// Returns true if the files should be rotated
  def write(buffers: Array[ByteBuffer]): Unit = {
    println(s"Writing ${buffers} to file ${files(active.index).fileId}")
    files(active.index).write(buffers)
  }

  def rotateFiles(): FileId = {

    val (new_active, retire) = active match {
      case Zero => (One,  Two)
      case One  => (Two,  Zero)
      case Two  => (Zero, One)
    }

    active = new_active
    files(active.index).resetFile()

    entry.setOffset(16)

    files(retire.index).fileId
  }
}
