package com.ibm.amoeba.common.transaction

import com.ibm.amoeba.common.HLCTimestamp
import com.ibm.amoeba.common.objects.{Key, KeyValueObjectPointer, ObjectPointer, ObjectRefcount, ObjectRevision}

sealed abstract class TransactionRequirement

case class LocalTimeRequirement(timestamp: HLCTimestamp,
                                tsRequirement: LocalTimeRequirement.Requirement.Value) extends TransactionRequirement

object LocalTimeRequirement {

  object Requirement extends Enumeration {
    val LessThan: Value = Value("LessThan")
    val GreaterThan: Value = Value("GreaterThan")
  }

  case class Requirement(timestamp: HLCTimestamp, tsRequirement: Requirement.Value)

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
                           requirements: List[KeyValueUpdate.KVRequirement],
                           timestamp: HLCTimestamp) extends KeyValueTransactionRequirement

object KeyValueUpdate {

  object TimestampRequirement extends Enumeration {
    val Equals: Value       = Value("Equals")
    val LessThan: Value     = Value("LessThan")
    val Exists: Value       = Value("Exists")
    val DoesNotExist: Value = Value("DoesNotExist")
  }

  case class KVRequirement(key: Key, timestamp: HLCTimestamp, tsRequirement: TimestampRequirement.Value)
}

