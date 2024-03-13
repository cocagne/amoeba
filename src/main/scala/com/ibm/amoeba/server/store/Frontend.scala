package com.ibm.amoeba.server.store

import java.util.UUID

import com.ibm.amoeba.common.HLCTimestamp
import com.ibm.amoeba.common.network.{Allocate, AllocateResponse, ClientId, OpportunisticRebuild, ReadResponse, TxAccept, TxFinalized, TxHeartbeat, TxMessage, TxPrepare, TxResolved, TxStatusRequest}
import com.ibm.amoeba.common.objects.{Metadata, ObjectId, ObjectRevision, ReadError}
import com.ibm.amoeba.common.store.{ReadState, StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.{TransactionDescription, TransactionId}
import com.ibm.amoeba.server.crl.{AllocSaveComplete, AllocationRecoveryState, CrashRecoveryLog, SaveCompletion, TransactionRecoveryState, TxSaveComplete}
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.store.backend.{Backend, Commit, CommitState, Completion, Read}
import com.ibm.amoeba.server.store.cache.ObjectCache
import com.ibm.amoeba.server.transaction.{TransactionStatusCache, Tx}
import org.apache.logging.log4j.scala.Logging


object Frontend {

  sealed abstract class ReadKind

  case class NetworkRead(clientId: ClientId, requestUUID: UUID) extends ReadKind

  case class TransactionRead(transactionId: TransactionId) extends ReadKind

  case class OpportuneRebuild(or: OpportunisticRebuild) extends ReadKind
}

class Frontend(val storeId: StoreId,
               val backend: Backend,
               val objectCache: ObjectCache,
               val net: Messenger,
               val crl: CrashRecoveryLog,
               val statusCache: TransactionStatusCache) extends Logging {

  import Frontend._

  var transactions: Map[TransactionId, Tx] = Map()

  private var pendingReads: Map[ObjectId, List[ReadKind]] = Map()
  private var pendingAllocations: Map[TransactionId, List[AllocateResponse]] = Map()
  private var allocationCommits: Set[TransactionId] = Set()

  {
    val (ltrs, lalloc) = crl.getFullRecoveryState(storeId)

    ltrs.foreach { trs =>
      val txd = TransactionDescription.deserialize(trs.serializedTxd)
      val locaters = txd.hostedObjectLocaters(storeId)
      val tx = new Tx(trs, txd, backend, net, crl, statusCache, Nil, locaters)
      transactions += (txd.transactionId -> tx)
    }

    lalloc.foreach { ars =>
      val msg = AllocateResponse(ClientId.Null, storeId, ars.allocationTransactionId,
        ars.newObjectId, Some(ars.storePointer))
      var l = pendingAllocations.get(ars.allocationTransactionId) match {
        case None => Nil
        case Some(lst) => lst
      }

      l = msg :: l

      pendingAllocations += (ars.allocationTransactionId -> l)
    }
  }

  def receiveTransactionMessage(msg: TxMessage): Unit = msg match {
    case m: TxPrepare => receivePrepare(m)
    case m: TxAccept => transactions.get(m.transactionId).foreach(tx => tx.receiveAccept(m))
    case m: TxResolved =>
      transactions.get(m.transactionId).foreach(tx => tx.receiveResolved(m))
      receiveResolved(m)
    case m: TxFinalized =>
      transactions.get(m.transactionId).foreach(tx => tx.receiveFinalized(m))
      transactions -= m.transactionId
    case m: TxHeartbeat => transactions.get(m.transactionId).foreach(tx => tx.receiveHeartbeat(m))
    case m: TxStatusRequest => transactions.get(m.transactionId).foreach(tx => tx.receiveStatusRequest(m))
    case _ => // Other TxMessages are not relevant
  }

  private def receivePrepare(m: TxPrepare): Unit = transactions.get(m.txd.transactionId) match {
    case Some(tx) =>
      logger.trace(s"**** EXISTING TX: ${m.txd.transactionId}. $tx")
      tx.receivePrepare(m)
    case None =>
      logger.trace(s"**** CREATING NEW TX: ${m.txd.transactionId}")
      val trs = TransactionRecoveryState.initial(m.to, m.txd, m.objectUpdates)
      val locaters = m.txd.hostedObjectLocaters(m.to)
      val tx = new Tx(trs, m.txd, backend, net, crl, statusCache, m.preTxRebuilds, locaters)
      transactions += (m.txd.transactionId -> tx)
      tx.receivePrepare(m)
      locaters.foreach(locater => readObjectForTransaction(tx, locater))
  }

  /** TxResolved messages are the one and only mechanism for resolving pending allocations */
  private def receiveResolved(m: TxResolved): Unit = {
    pendingAllocations.get(m.transactionId).foreach { lst =>
      lst.foreach { ars =>
        // Guaranteed to be in the cache due to the transaction reference
        val os = objectCache.get(ars.newObjectId).get

        // Remove the reference count we added when this ObjectState was created
        os.transactionReferences -= 1

        if (m.committed) {
          val cs = CommitState(os.objectId, os.storePointer, os.metadata, os.objectType, os.data, os.maxSize)
          allocationCommits += m.transactionId
          backend.commit(cs, m.transactionId)
        } else {
          crl.deleteAllocation(storeId, m.transactionId)
          objectCache.remove(os.objectId)
          backend.abortAllocation(os.objectId)
        }
      }

      pendingAllocations -= m.transactionId
    }
  }

  def backendOperationComplete(completion: Completion): Unit = completion match {
    case r: Read => backendReadComplete(r.objectId, r.storePointer, r.result)
    case c: Commit =>
      transactions.get(c.transactionId).foreach { tx =>
        tx.commitComplete(c.objectId, c.result)
      }

      if (allocationCommits.contains(c.transactionId)) {
        allocationCommits -= c.transactionId
        crl.deleteAllocation(storeId, c.transactionId)
      }
  }

  def readObjectForNetwork(clientId: ClientId, readUUID: UUID, locater: Locater): Unit = {
    objectCache.get(locater.objectId) match {
      case Some(os) =>
        logger.trace(s"Reading Cached object for read $readUUID. Revision ${os.metadata.revision}")
        val cs = ReadResponse.CurrentState(os.metadata.revision, os.metadata.refcount, os.metadata.timestamp,
          os.data.size, Some(os.data), os.lockedWriteTransactions )

        val rr = ReadResponse(clientId, storeId, readUUID, HLCTimestamp.now, Right(cs) )
        net.sendClientResponse(rr)


      case None =>
        logger.trace(s"Reading uncached object from backing store for read $readUUID")
        val nr = NetworkRead(clientId, readUUID)

        pendingReads.get(locater.objectId) match {
          case Some(lst) =>
            pendingReads += (locater.objectId -> (nr :: lst))

          case None =>
            pendingReads += (locater.objectId -> (nr :: Nil))
            backend.read(locater)
        }
    }
  }

  def readObjectForOpportunisticRebuild(op: OpportunisticRebuild): Unit = {
    objectCache.get(op.pointer.id) match {
      case Some(os) => opportunisticRebuild(op, os)
      case None =>
        op.pointer.getStoreLocater(storeId).foreach {locater =>
          pendingReads.get(locater.objectId) match {
            case Some(lst) =>
              pendingReads += (locater.objectId -> (OpportuneRebuild(op) :: lst))

            case None =>
              pendingReads += (locater.objectId -> (OpportuneRebuild(op) :: Nil))
              backend.read(locater)
          }
        }
    }
  }

  private def readObjectForTransaction(transaction: Tx, locater: Locater): Unit = {
    objectCache.get(locater.objectId) match {
      case Some(os) => transaction.objectLoaded(os)
      case None =>
        val tr = TransactionRead(transaction.transactionId)

        pendingReads.get(locater.objectId) match {
          case Some(lst) =>
            pendingReads += (locater.objectId -> (tr :: lst))

          case None =>
            pendingReads += (locater.objectId -> (tr :: Nil))
            backend.read(locater)
        }
    }
  }

  private def opportunisticRebuild(op: OpportunisticRebuild, os: ObjectState): Unit = {
    if (os.metadata.revision == op.revision) {
      val rc = if (op.refcount.updateSerial > os.metadata.refcount.updateSerial)
        op.refcount
      else
        os.metadata.refcount

      os.metadata = Metadata(op.revision, rc, op.timestamp)
      os.data = op.data
      val cs = CommitState(os.objectId, os.storePointer, os.metadata, os.objectType, os.data, os.maxSize)
      val txid = TransactionId(op.revision.lastUpdateTxUUID)
      // No need to wait for this to complete
      backend.commit(cs, txid)
    }
  }

  def backendReadComplete(objectId: ObjectId,
                          storePointer: StorePointer,
                          result: Either[ReadState, ReadError.Value]): Unit = {
    result match {
      case Left(rs) =>
        val os = new ObjectState(objectId, storePointer, rs.metadata, rs.objectType, rs.data, None)
        objectCache.insert(os)

        pendingReads.get(objectId).foreach { lpr =>
          lpr.foreach {
            case netRead: NetworkRead =>
              logger.trace(s"Completed backend load for read ${netRead.requestUUID}. SUCCESS. Sending read response")
              val cs = ReadResponse.CurrentState(os.metadata.revision, os.metadata.refcount, os.metadata.timestamp,
                os.data.size, Some(os.data), os.lockedWriteTransactions )

              val rr = ReadResponse(netRead.clientId, storeId, netRead.requestUUID, HLCTimestamp.now, Right(cs) )
              net.sendClientResponse(rr)

            case tr: TransactionRead => transactions.get(tr.transactionId).foreach { tx => tx.objectLoaded(os) }

            case OpportuneRebuild(op) => opportunisticRebuild(op, os)
          }
        }

      case Right(err) =>
        pendingReads.get(objectId).foreach { lpr =>
          lpr.foreach {
            case netRead: NetworkRead =>
              logger.trace(s"Completed backend load for read ${netRead.requestUUID}. ERROR $err. Sending read response")
              val rr = ReadResponse(netRead.clientId, storeId, netRead.requestUUID, HLCTimestamp.now, Left(err) )
              net.sendClientResponse(rr)

            case tr: TransactionRead => transactions.get(tr.transactionId).foreach { tx => tx.objectLoadFailed(objectId, err) }

            case _: OpportuneRebuild => // Can't guarantee correctness so we need to ignore this
          }
        }
    }

    pendingReads -= objectId
  }

  def crlSaveComplete(c: SaveCompletion): Unit = c match {
    case t: TxSaveComplete =>
      transactions.get(t.transactionId).foreach { tx =>
        tx.crlSaveComplete(t.saveId)
      }

    case a: AllocSaveComplete =>
      pendingAllocations.get(a.transactionId).foreach { lst =>
        lst.foreach(msg => {
          logger.trace(s"CRL Save Completed for Allocation of object ${msg.newObjectId}")
          net.sendClientResponse(msg)
        })
      }
  }

  def allocateObject(msg: Allocate): Unit = {
    // Check to see if we've already received an allocation request for this object
    pendingAllocations.get(msg.allocationTransactionId) match {
      case Some(lst) =>
        if (lst.exists(p => p.newObjectId == msg.newObjectId))
          return
      case None =>
    }

    val metadata = Metadata(ObjectRevision(msg.allocationTransactionId),
      msg.initialRefcount, msg.timestamp)

    val either = backend.allocate(msg.newObjectId, msg.objectType, metadata, msg.objectData, msg.objectSize)

    either match {
      case Right(_) =>
        val r = AllocateResponse(msg.fromClient, msg.toStore, msg.allocationTransactionId, msg.newObjectId, None)
        net.sendClientResponse(r)

      case Left(storePointer) =>
        logger.trace(s"Backend allocated new object ${msg.newObjectId}. Saving in CRL")
        val r = AllocateResponse(msg.fromClient, msg.toStore, msg.allocationTransactionId, msg.newObjectId,
          Some(storePointer))

        val arList = pendingAllocations.get(msg.allocationTransactionId) match {
          case Some(lst) => r :: lst
          case None => r :: Nil
        }

        pendingAllocations += (msg.allocationTransactionId -> arList)

        val os = new ObjectState(msg.newObjectId, storePointer, metadata, msg.objectType, msg.objectData,
          msg.objectSize)

        // Ensure this object stays in the cache
        os.transactionReferences += 1

        objectCache.insert(os)

        val ars = AllocationRecoveryState(
          storeId,
          storePointer,
          msg.newObjectId,
          msg.objectType,
          msg.objectSize,
          msg.objectData,
          msg.initialRefcount,
          msg.timestamp,
          msg.allocationTransactionId,
          msg.revisionGuard.serialize()
        )

        crl.save(ars)
    }
  }
}
