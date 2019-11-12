package com.ibm.amoeba.common.transaction

import com.ibm.amoeba.common.HLCTimestamp
import com.ibm.amoeba.common.objects.{Key, KeyValueObjectPointer, ObjectPointer, ObjectRefcount, ObjectRevision}

sealed abstract class TransactionRequirement

case class LocalTimeRequirement(timestamp: HLCTimestamp,
                                tsRequirement: LocalTimeRequirement.Requirement.Value) extends TransactionRequirement

object LocalTimeRequirement {

  object Requirement extends Enumeration {
    val Equals: Value = Value("Equals")
    val LessThan: Value = Value("LessThan")
    val GreaterThan: Value = Value("GreaterThan")
  }
}


sealed abstract class TransactionObjectRequirement extends TransactionRequirement {
  val objectPointer: ObjectPointer
}

case class DataUpdate(
                       objectPointer: ObjectPointer,
                       requiredRevision: ObjectRevision,
                       operation: DataUpdateOperation.Value) extends TransactionObjectRequirement

case class RefcountUpdate(
                           objectPointer: ObjectPointer,
                           requiredRefcount: ObjectRefcount,
                           newRefcount: ObjectRefcount) extends TransactionObjectRequirement

case class VersionBump(
                        objectPointer: ObjectPointer,
                        requiredRevision: ObjectRevision) extends TransactionObjectRequirement

case class RevisionLock(
                         objectPointer: ObjectPointer,
                         requiredRevision: ObjectRevision) extends TransactionObjectRequirement

sealed abstract class KeyValueTransactionRequirement extends TransactionObjectRequirement {
  override val objectPointer: KeyValueObjectPointer
}

case class KeyValueUpdate(
                           objectPointer: KeyValueObjectPointer,
                           requiredRevision: Option[ObjectRevision],
                           requirements: List[KeyValueUpdate.KeyRequirement]) extends KeyValueTransactionRequirement

object KeyValueUpdate {

  sealed abstract class KeyRequirement {
    val key: Key
  }

  case class Exists(key: Key) extends KeyRequirement
  case class MayExist(key: Key) extends KeyRequirement
  case class DoesNotExist(key: Key) extends KeyRequirement
  case class TimestampEquals(key: Key, timestamp: HLCTimestamp) extends KeyRequirement
  case class TimestampLessThan(key: Key, timestamp: HLCTimestamp) extends KeyRequirement
  case class TimestampGreaterThan(key: Key, timestamp: HLCTimestamp) extends KeyRequirement

}

