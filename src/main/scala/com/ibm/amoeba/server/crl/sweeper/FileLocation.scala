package com.ibm.amoeba.server.crl.sweeper

class FileLocation(val fileId: FileId, val offset: Long, val length: Int)

object FileLocation{
  val Null = new FileLocation(new FileId(0), offset = 0, length = 0)
}