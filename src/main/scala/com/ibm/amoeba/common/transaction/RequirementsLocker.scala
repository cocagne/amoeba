package com.ibm.amoeba.common.transaction

import com.ibm.amoeba.common.objects.{ObjectId, ObjectRevision}
import com.ibm.amoeba.common.store.{ObjectState, ValueState}
import com.ibm.amoeba.common.transaction.RequirementsChecker.ObjectErr

import scala.collection.immutable.HashMap

object RequirementsLocker {

  /** Locks all objects hosted by this store to the transaction. This method may be
    * called ONLY after a call to RequirementsChecker.check returns ZERO errors
    */
  def lock(transactionId: TransactionId,
            requirements: List[TransactionRequirement],
            objects: HashMap[ObjectId, ObjectState]): Unit = {

    def getState(oid: ObjectId): ObjectState = {
      objects.get(oid) match {
        case None => throw ObjectErr(oid, RequirementError.MissingObject)
        case Some(os) => os
      }
    }

    for (req <- requirements) {
      try {
        req match {
          case r: LocalTimeRequirement =>

          case r: DataUpdate => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)

          case r: KeyValueUpdate =>
            lockKV(getState(r.objectPointer.id), transactionId, r.requiredRevision, r.requirements)

          case r: RefcountUpdate => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)

          case r: VersionBump => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)

          case r: RevisionLock => getState(r.objectPointer.id).lockedToTransaction = Some(transactionId)
        }
      } catch {
        case e: Throwable => println(s"UNEXPECTED ERROR IN TX Lock: $e")
      }
    }
  }

  def lockKV(state: ObjectState,
             transactionId: TransactionId,
             requiredRevision: Option[ObjectRevision],
             keyRequirements: List[KeyValueUpdate.KeyRequirement]): Unit = {

    requiredRevision.foreach { rev => state.lockedToTransaction = Some(transactionId) }

    def checkLock(vs: ValueState): Unit = {
      vs.lockedToTransaction.foreach { lockedTransactionId =>
        if (lockedTransactionId != transactionId)
          throw ObjectErr(state.objectId, RequirementError.TransactionCollision)
      }
    }

    state.kvState match {
      case None => throw ObjectErr(state.objectId, RequirementError.ObjectTypeError)
      case Some(kvs) =>
        for (req <- keyRequirements) {
          req match {
            case r: KeyValueUpdate.Exists => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = Some(transactionId)
            }

            case r: KeyValueUpdate.MayExist => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = Some(transactionId)
            }

            case r: KeyValueUpdate.DoesNotExist => kvs.noExistenceLocks += r.key

            case r: KeyValueUpdate.TimestampEquals => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = Some(transactionId)
            }

            case r: KeyValueUpdate.TimestampLessThan => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = Some(transactionId)
            }

            case r: KeyValueUpdate.TimestampGreaterThan => kvs.content.get(r.key).foreach { vs =>
              vs.lockedToTransaction = Some(transactionId)
            }
          }
        }
    }
  }
}
