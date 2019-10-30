package com.ibm.amoeba.server.crl.sweeper

import java.nio.ByteBuffer
import java.nio.file.Path

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

object TriFileStream {

  def createLogFile(directory: Path, fileId: FileId, maxSize: Long): LogFile = {
    new LogFile(directory.resolve(s"${fileId.number}"), fileId, maxSize)
  }

}

class TriFileStream(directory: Path, startingFileId: FileId, val maxSize: Long) {
  import TriFileStream._

  val files = new Array[LogFile](3)
  files(0) = createLogFile(directory, startingFileId, maxSize)
  files(1) = createLogFile(directory, new FileId(startingFileId.number + 1), maxSize)
  files(2) = createLogFile(directory, new FileId(startingFileId.number + 2), maxSize)

  private var active: Index = Zero

  {
    var highest = files(0).findLastValidEntry() match {
      case None => new LogEntrySerialNumber(0)
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

  def status(): (FileId, Long) = {
    val f = files(active.index)
    (f.fileId, f.size)
  }

  def write(buffers: Array[ByteBuffer]): Unit = {
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

    files(retire.index).fileId
  }
}
