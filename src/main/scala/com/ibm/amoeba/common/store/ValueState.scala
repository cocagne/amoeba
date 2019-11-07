package com.ibm.amoeba.common.store

import com.ibm.amoeba.common.HLCTimestamp
import com.ibm.amoeba.common.objects.{ObjectRevision, Value}
import com.ibm.amoeba.common.transaction.TransactionId

class ValueState(var value: Value,
                 var revision: ObjectRevision,
                 var timestamp: HLCTimestamp,
                 var lockedToTransaction: Option[TransactionId]) {

}
