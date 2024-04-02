package com.ibm.amoeba.server.crl.simple

case class StreamId(number: Int) extends AnyVal

object StreamId:
  // 1 Byte file index number
  val StaticSize: Int = 1
