package com.ibm.amoeba.common.transaction

object RequirementError extends Enumeration {
  val TransactionCollision: Value = Value
  val LocalTimeError: Value = Value
  val MissingObject: Value = Value
  val MissingObjectUpdate: Value = Value
  val ObjectTypeError: Value = Value
  val RevisionMismatch: Value = Value
  val RefcountMismatch: Value = Value
  val KeyTimestampError: Value = Value
  val KeyExistenceError: Value = Value
}
