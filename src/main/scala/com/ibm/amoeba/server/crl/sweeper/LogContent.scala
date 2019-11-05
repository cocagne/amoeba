package com.ibm.amoeba.server.crl.sweeper

import com.ibm.amoeba.server.crl.AllocationRecoveryState
import com.ibm.amoeba.server.crl.TransactionRecoveryState

sealed abstract class LogContent(var lastEntrySerial: LogEntrySerialNumber) extends Ordered[LogContent] {
  var next: Option[LogContent] = None
  var prev: Option[LogContent] = None

  def compare(that: LogContent): Int = {
    this.lastEntrySerial.compare(that.lastEntrySerial)
  }
}

class Tx(val id: TxId,
         var state: TransactionRecoveryState,
         lastEntrySerial: LogEntrySerialNumber,
         var txdLocation: Option[FileLocation] = None,
         var objectUpdateLocations: List[FileLocation] = Nil,
         var keepObjectUpdates: Boolean = true) extends LogContent(lastEntrySerial) {
}

class Alloc( var dataLocation: Option[FileLocation],
             var state: AllocationRecoveryState,
             lastEntrySerial: LogEntrySerialNumber ) extends LogContent(lastEntrySerial)