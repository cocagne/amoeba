package com.ibm.amoeba.server.crl.sweeper

case class LogEntrySerialNumber(number: Long) extends Ordered[LogEntrySerialNumber] {
  def next(): LogEntrySerialNumber = {
    LogEntrySerialNumber(number + 1)
  }

  def compare(that: LogEntrySerialNumber): Int = {
    (this.number - that.number).asInstanceOf[Int]
  }
}
