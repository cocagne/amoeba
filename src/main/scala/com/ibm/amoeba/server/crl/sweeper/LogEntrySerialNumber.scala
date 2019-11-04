package com.ibm.amoeba.server.crl.sweeper

case class LogEntrySerialNumber(number: Long) extends AnyVal {
  def next(): LogEntrySerialNumber = {
    new LogEntrySerialNumber(number + 1)
  }
}
