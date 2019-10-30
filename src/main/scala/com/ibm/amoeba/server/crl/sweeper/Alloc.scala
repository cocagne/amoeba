package com.ibm.amoeba.server.crl.sweeper

import com.ibm.amoeba.server.crl.AllocationRecoveryState

class Alloc( var dataLocation: Option[FileLocation],
             var state: AllocationRecoveryState,
             var lastEntrySerial: LogEntrySerialNumber )
