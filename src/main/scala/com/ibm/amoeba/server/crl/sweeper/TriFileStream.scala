package com.ibm.amoeba.server.crl.sweeper

import java.nio.ByteBuffer
import java.nio.file.Path
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


class TriFileStream(file0: LogFile, file1: LogFile, file2: LogFile) {

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

  def status(): (FileId, UUID) = {
    val f = files(active.index)
    (f.fileId, f.fileUUID)
  }

  /// Returns true if the files should be rotated
  def write(buffers: Array[ByteBuffer]): Boolean = {
    files(active.index).write(buffers)
    files(active.index).size < maxSize - 4096*2
  }

  def rotateFiles(): FileId = {

    val (new_active, retire) = active match {
      case Zero => (One,  Two)
      case One  => (Two,  Zero)
      case Two  => (Zero, One)
    }

    active = new_active
    files(active.index).resetFile()

    files(retire.index).fileId
  }
}
