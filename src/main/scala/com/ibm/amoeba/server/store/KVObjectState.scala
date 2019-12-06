package com.ibm.amoeba.server.store

import com.ibm.amoeba.common.objects.Key
import com.ibm.amoeba.common.transaction.TransactionId

class KVObjectState {
  var min: Option[Key] = None
  var max: Option[Key] = None
  var left: Option[Key] = None
  var right: Option[Key] = None
  var content: Map[Key, ValueState] = Map()
  var noExistenceLocks: Map[Key, TransactionId] = Map()
}
