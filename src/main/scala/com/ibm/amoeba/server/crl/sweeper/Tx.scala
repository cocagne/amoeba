package com.ibm.amoeba.server.crl.sweeper

import com.ibm.amoeba.server.crl.TransactionRecoveryState

class Tx( val id: TxId,
          var state: TransactionRecoveryState,
          var lastEntrySerial: LogEntrySerialNumber,
          var txdLocation: Option[FileLocation] = None,
          var objectUpdateLocations: List[FileLocation] = Nil,
          var keepObjectUpdates: Boolean = true) {
}
