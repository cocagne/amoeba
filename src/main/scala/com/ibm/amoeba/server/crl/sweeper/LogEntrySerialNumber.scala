package com.ibm.amoeba.server.crl.sweeper

class LogEntrySerialNumber(val number: Long) extends AnyVal {
  def next(): LogEntrySerialNumber = {
    new LogEntrySerialNumber(number + 1)
  }
}
