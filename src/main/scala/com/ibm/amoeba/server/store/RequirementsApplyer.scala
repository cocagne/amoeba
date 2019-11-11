package com.ibm.amoeba.server.store

import com.ibm.amoeba.common.objects._
import com.ibm.amoeba.common.transaction._
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}

import scala.collection.immutable.HashMap

class RequirementsApplyer {

  /**
    * @return Tuple of object specific errors and a list of non-object errors
    */
  def apply(transactionId: TransactionId,
            timestamp: HLCTimestamp,
            requirements: List[TransactionRequirement],
            objects: HashMap[ObjectId, ObjectState],
            objectUpdates: HashMap[ObjectId, DataBuffer]): Set[ObjectId] = {

    // Get a list of all objects that are unfit for accepting the changes in this transaction
    val skippedObjects = RequirementsChecker.check(transactionId, requirements, objects, objectUpdates)._1.keySet

    // Filter out all non-object requirements and any objects not fit for accepting tx changes
    val reqIter = requirements.iterator.filter(r => r.isInstanceOf[TransactionObjectRequirement]).map { r =>
      val tr = r.asInstanceOf[TransactionObjectRequirement]
      (tr.objectPointer, tr)
    }.filter { t => !skippedObjects.contains(t._1.id) }

    for ((ptr, req) <- reqIter) {
      try {
        req match {

          case r: DataUpdate =>
            val s = objects(ptr.id)
            s.metadata = s.metadata.copy(
              revision=ObjectRevision(transactionId),
              timestamp=timestamp)

            r.operation match {
              case DataUpdateOperation.Overwrite => s.data = objectUpdates(ptr.id)
              case DataUpdateOperation.Append => s.data = s.data.append(objectUpdates(ptr.id))
            }

          case r: KeyValueUpdate =>

            val state = objects(ptr.id)
            val kvs = state.kvState.get

            r.requiredRevision.foreach { _ =>
              state.metadata = state.metadata.copy(revision=ObjectRevision(transactionId), timestamp=timestamp)
            }

            val kvops = KeyValueOperation.decode(objectUpdates(ptr.id), ObjectRevision(transactionId), timestamp)

            for (op <- kvops) {
              op match {
                case o: SetMin => kvs.min = Some(Key(o.value))
                case o: SetMax => kvs.max = Some(Key(o.value))
                case o: SetLeft => kvs.left = Some(Key(o.value))
                case o: SetRight => kvs.right = Some(Key(o.value))
                case _: DeleteMin => kvs.min = None
                case _: DeleteMax => kvs.max = None
                case _: DeleteLeft => kvs.left = None
                case _: DeleteRight => kvs.right = None
                case o: Insert =>
                  val vs = new ValueState(Value(o.value), ObjectRevision(transactionId), timestamp, None)
                  kvs.content += (o.key -> vs)
                case o: Delete => kvs.content -= o.key
              }
            }

          case r: RefcountUpdate =>
            val s = objects(ptr.id)
            s.metadata = s.metadata.copy(
              refcount=r.newRefcount,
              timestamp=timestamp)

          case _: VersionBump =>
            val s = objects(ptr.id)
            s.metadata = s.metadata.copy(
              revision=ObjectRevision(transactionId),
              timestamp=timestamp)

          case _: RevisionLock =>
        }
      } catch {
        case e: Throwable => println(s"UNEXPECTED ERROR IN TX Apply: $e")
      }
    }

    skippedObjects
  }
}
