package com.ibm.amoeba.server.cnc


sealed abstract class CnCReply

case class Ok() extends CnCReply

case class Error(message: String) extends CnCReply

