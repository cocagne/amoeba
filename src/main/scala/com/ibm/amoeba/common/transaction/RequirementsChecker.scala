package com.ibm.amoeba.common.transaction

import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.objects.{ObjectId, ObjectRefcount, ObjectRevision}
import com.ibm.amoeba.common.store.{ObjectState, ValueState}

import scala.collection.immutable.HashMap

object RequirementsChecker {

  case class ObjectErr(objectId: ObjectId, err: RequirementError.Value) extends Exception
  case class NonObjectErr(err: RequirementError.Value) extends Exception

  /**
    * @return Tuple of object specific errors an list of non-object errors
    */
  def check(transactionId: TransactionId,
            requirements: List[TransactionRequirement],
            objects: HashMap[ObjectId, ObjectState],
            objectUpdates: HashMap[ObjectId, DataBuffer]):
  (HashMap[ObjectId, RequirementError.Value], List[RequirementError.Value]) = {

    def getState(oid: ObjectId): ObjectState = {
      objects.get(oid) match {
        case None => throw ObjectErr(oid, RequirementError.MissingObject)
        case Some(os) =>
          os.lockedToTransaction match {
            case None => os
            case Some(lockedTxId) =>
              if (lockedTxId != transactionId)
                throw ObjectErr(oid, RequirementError.TransactionCollision)
              else
                os
          }
      }
    }

    var objectErrors: HashMap[ObjectId, RequirementError.Value] = new HashMap()
    var nonObjectErrors: List[RequirementError.Value] = Nil

    for (req <- requirements) {
      try {
        req match {
          case r: LocalTimeRequirement => checkLocalTime(r)

          case r: DataUpdate =>
            checkRevision(getState(r.objectPointer.id), r.requiredRevision)
            if (!objectUpdates.contains(r.objectPointer.id))
              throw ObjectErr(r.objectPointer.id, RequirementError.MissingObjectUpdate)

          case r: KeyValueUpdate =>
            checkKVRequirements(getState(r.objectPointer.id), transactionId, r.requiredRevision, r.requirements)
            if (!objectUpdates.contains(r.objectPointer.id))
              throw ObjectErr(r.objectPointer.id, RequirementError.MissingObjectUpdate)

          case r: RefcountUpdate => checkRefcount(getState(r.objectPointer.id), r.requiredRefcount)

          case r: VersionBump => checkRevision(getState(r.objectPointer.id), r.requiredRevision)

          case r: RevisionLock => checkRevision(getState(r.objectPointer.id), r.requiredRevision)
        }
      } catch {
        case err: ObjectErr => objectErrors += (err.objectId -> err.err)
        case err: NonObjectErr => nonObjectErrors = err.err :: nonObjectErrors
        case e: Throwable => println(s"UNEXPECTED ERROR IN TX REQ CHECK: $e")
      }
    }

    (objectErrors, nonObjectErrors)
  }

  def checkLocalTime(tsReq: LocalTimeRequirement): Unit = {
    val now = HLCTimestamp.now
    tsReq.tsRequirement match {
      case LocalTimeRequirement.Requirement.Equals =>
        if (now != tsReq.timestamp)
          throw NonObjectErr(RequirementError.LocalTimeError)

      case LocalTimeRequirement.Requirement.LessThan =>
        if (now >= tsReq.timestamp)
          throw NonObjectErr(RequirementError.LocalTimeError)

      case LocalTimeRequirement.Requirement.GreaterThan =>
        if (now <= tsReq.timestamp)
          throw NonObjectErr(RequirementError.LocalTimeError)
    }
  }

  def checkRevision(state: ObjectState, requiredRevision: ObjectRevision): Unit = {
    if (state.metadata.revision != requiredRevision)
      throw ObjectErr(state.objectId, RequirementError.RevisionMismatch)
  }

  def checkRefcount(state: ObjectState, requiredRefcount: ObjectRefcount): Unit = {
    if (state.metadata.refcount != requiredRefcount)
      throw ObjectErr(state.objectId, RequirementError.RefcountMismatch)
  }

  def checkKVRequirements(state: ObjectState,
                          transactionId: TransactionId,
                          requiredRevision: Option[ObjectRevision],
                          keyRequirements: List[KeyValueUpdate.KeyRequirement]): Unit = {

    requiredRevision.foreach { rev =>
      if (rev != state.metadata.revision)
        throw ObjectErr(state.objectId, RequirementError.RevisionMismatch)
    }

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
            case r: KeyValueUpdate.Exists => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, RequirementError.KeyExistenceError)
              case Some(vs) => checkLock(vs)
            }

            case r: KeyValueUpdate.MayExist => kvs.content.get(r.key) match {
              case None =>
                if (kvs.noExistenceLocks.contains(r.key))
                  throw ObjectErr(state.objectId, RequirementError.KeyExistenceError)
              case Some(vs) => checkLock(vs)
            }

            case r: KeyValueUpdate.DoesNotExist => kvs.content.get(r.key) match {
              case None => if (kvs.noExistenceLocks.contains(r.key))
                throw ObjectErr(state.objectId, RequirementError.TransactionCollision)
              case Some(_) => throw ObjectErr(state.objectId, RequirementError.KeyExistenceError)
            }

            case r: KeyValueUpdate.TimestampEquals => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, RequirementError.KeyExistenceError)
              case Some(vs) =>
                if (vs.timestamp != r.timestamp)
                  throw ObjectErr(state.objectId, RequirementError.KeyTimestampError)
                checkLock(vs)
            }

            case r: KeyValueUpdate.TimestampLessThan => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, RequirementError.KeyExistenceError)
              case Some(vs) =>
                if (vs.timestamp < r.timestamp)
                  throw ObjectErr(state.objectId, RequirementError.KeyTimestampError)
                checkLock(vs)
            }

            case r: KeyValueUpdate.TimestampGreaterThan => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, RequirementError.KeyExistenceError)
              case Some(vs) =>
                if (vs.timestamp > r.timestamp)
                  throw ObjectErr(state.objectId, RequirementError.KeyTimestampError)
                checkLock(vs)
            }
          }
        }
    }
  }
}
