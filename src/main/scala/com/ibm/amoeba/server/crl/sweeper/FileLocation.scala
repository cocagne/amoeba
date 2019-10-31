package com.ibm.amoeba.server.crl.sweeper

case class FileLocation(fileId: FileId, offset: Long, length: Int)

object FileLocation{
  val Null = new FileLocation(new FileId(0), offset = 0, length = 0)
}