package com.ibm.amoeba.common.transaction

object TransactionStatus extends Enumeration {
  val Unresolved: Value = Value("Unresolved")
  val Committed: Value  = Value("Committed")
  val Aborted: Value    = Value("Aborted")
}
