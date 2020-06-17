package com.ibm.amoeba.server.transaction

import com.ibm.amoeba.common.network.{TxAccept, TxAcceptResponse, TxCommitted, TxFinalized, TxHeartbeat, TxPrepare, TxPrepareResponse, TxResolved, TxStatusRequest, TxStatusResponse}
import com.ibm.amoeba.common.objects.{ObjectId, ReadError}
import com.ibm.amoeba.common.paxos.{Accept, Acceptor, Prepare}
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{ObjectUpdate, PreTransactionOpportunisticRebuild, RequirementError, TransactionCollision, TransactionDescription, TransactionDisposition, TransactionId, TransactionStatus}
import com.ibm.amoeba.server.crl.{CrashRecoveryLog, TransactionRecoveryState, TxSaveId}
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.store.backend.{Backend, CommitError, CommitState}
import com.ibm.amoeba.server.store.{Locater, ObjectState, RequirementsApplyer, RequirementsChecker, RequirementsLocker}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.duration._

object Tx {
  class DelayedPrepareResponse {
    var response: Option[TxPrepareResponse] = None
    var saveId: TxSaveId = TxSaveId(0)
  }

  class DelayedAcceptResponse {
    var response: Option[TxAcceptResponse] = None
    var saveId: TxSaveId = TxSaveId(0)
  }
}

class Tx( trs: TransactionRecoveryState,
          val txd: TransactionDescription,
          private val backend: Backend,
          private val net: Messenger,
          private val crl: CrashRecoveryLog,
          private val statusCache: TransactionStatusCache,
          private val preTxRebuilds: List[PreTransactionOpportunisticRebuild],
          private val objectLocaters: List[Locater]) extends Logging {

  import Tx._

  val transactionId: TransactionId = txd.transactionId

  private val storeId = trs.storeId
  private val serializedTxd = trs.serializedTxd
  private val objectUpdates = trs.objectUpdates
  private var disposition = trs.disposition
  private var collisions: List[TransactionId] = Nil
  private var status = trs.status
  private var oresolution: Option[Boolean] = None
  private var ofinalized: Option[Boolean] = None
  private var objects: Map[ObjectId, ObjectState] = Map()
  private var pendingObjectLoads: Int = objectLocaters.size
  private var pendingObjectCommits: Int = 0

  private val acceptor = new Acceptor(storeId.poolIndex, trs.paxosAcceptorState)
  private val delayedPrepare = new DelayedPrepareResponse
  private val delayedAccept = new DelayedAcceptResponse
  private var lastEvent: Long = System.nanoTime()
  private var saveObjectUpdates: Boolean = true
  private var nextCrlSave: TxSaveId = TxSaveId(1)
  private var committed: Boolean = false
  private var committing: Boolean = false
  private var locked: Boolean = false
  private var lastProposer: StoreId = StoreId(txd.primaryObject.poolId, txd.designatedLeaderUID)
  private var objectCommitErrors: List[ObjectId] = Nil

  statusCache.updateStatus(transactionId, status)

  private def allObjectsLoaded: Boolean = pendingObjectLoads == 0

  private def updateLastEvent(): Unit = lastEvent = System.nanoTime()

  def lastEventTime: Long = lastEvent

  def durationSinceLastEvent: Duration = Duration(System.nanoTime() - lastEvent, NANOSECONDS)

  private def nextCrlSaveId(): TxSaveId = {
    val id = nextCrlSave
    nextCrlSave = TxSaveId(id.number + 1)
    id
  }

  private def unlock(): Unit = if (locked) {
    locked = false

    RequirementsLocker.unlock(transactionId, txd.requirements, objects)
  }


  private def trsObjectUpdates: List[ObjectUpdate] = if (saveObjectUpdates) objectUpdates else Nil

  def objectLoaded(os: ObjectState): Unit = {
    objects += (os.objectId -> os)

    os.transactionReferences += 1 // Ensure this object stays in memory while the transaction is in progress

    pendingObjectLoads -= 1

    if (allObjectsLoaded)
      onAllObjectsLoaded()
  }

  def objectLoadFailed(objectId: ObjectId, err: ReadError.Value): Unit = {
    logger.warn(s"Failed to load object $objectId: $err")

    pendingObjectLoads -= 1

    if (allObjectsLoaded)
      onAllObjectsLoaded()
  }

  private def onAllObjectsLoaded(): Unit = {
    // Apply pre-transaction rebuilds
    preTxRebuilds.foreach { r =>
      objects.get(r.objectId).foreach { os =>
        if (os.metadata.revision == r.requiredMetadata.revision) {
          os.metadata = r.requiredMetadata
          os.data = r.data
          val cs = CommitState(os.objectId, os.storePointer, os.metadata, os.objectType, os.data, os.maxSize)
          val txid = TransactionId(r.requiredMetadata.revision.lastUpdateTxUUID)
          // No need to wait for this to complete
          backend.commit(cs, txid)
        }
      }
    }

    // Determine local disposition
    val ou = objectUpdates.iterator.map(ou => ou.objectId -> ou.data).toMap

    val (m, l) = RequirementsChecker.check(transactionId, txd.startTimestamp, txd.requirements, objects, ou)

    // Find all objects with transaction collisions for inclusion into the response message
    m.foreach { t => t._2 match {
      case TransactionCollision(txid) => collisions = txid :: collisions
      case _ =>
    }}

    disposition = if (m.isEmpty && l.isEmpty) TransactionDisposition.VoteCommit else TransactionDisposition.VoteAbort

    // Case Resolution Complete
    oresolution.foreach { resolution =>
      resolvedAndAllObjectsLoaded(resolution)
    }

    // Case received prepare before the objects are loaded. This will always be true for the first prepare
    // message as the receivePrepare() method is called immediately after transaction creation.

    delayedPrepare.response.foreach { response =>
      val r = response.copy(disposition=disposition, collisions=collisions)
      delayedPrepare.response = Some(r)
      delayedPrepare.saveId = nextCrlSaveId()
      val state = TransactionRecoveryState(storeId, serializedTxd, trsObjectUpdates,
        disposition, status, acceptor.persistentState)
      crl.save(txd.transactionId, state, delayedPrepare.saveId)
    }
  }

  private def doCommit(): Unit = if (!committed && !committing && allObjectsLoaded) {
    committing = true

    unlock()

    val ou = objectUpdates.iterator.map(ou => ou.objectId -> ou.data).toMap

    val skipped = RequirementsApplyer.apply(transactionId, txd.startTimestamp, txd.requirements,
      objects, ou)

    pendingObjectCommits = objects.size - skipped.size

    objects.valuesIterator.foreach { os =>
      if (!skipped.contains(os.objectId)) {
        val cs = CommitState(os.objectId, os.storePointer, os.metadata, os.objectType, os.data, os.maxSize)

        backend.commit(cs, transactionId)
      }
    }

    objectCommitErrors = skipped.toList
  }

  private def resolvedAndAllObjectsLoaded(committed: Boolean): Unit = {
    if (committed)
      doCommit()
    else
      crl.deleteTransaction(storeId, transactionId)

    unlock() // Ensure we've unlocked the objects now that result of the Tx is known

    // No need to force the objects referenced by this transaction to stay in memory.
    // Release the reference locking them into the cache
    objects.valuesIterator.foreach { os =>
      os.transactionReferences -= 1
    }
  }

  private def resolved(committed: Boolean): Unit = if (oresolution.isEmpty) {
    oresolution = Some(committed)

    status = if (committed)
      TransactionStatus.Committed
    else
      TransactionStatus.Aborted

    statusCache.updateStatus(transactionId, status)

    if (allObjectsLoaded)
      resolvedAndAllObjectsLoaded(committed)
  }

  def receivePrepare(m: TxPrepare): Unit = {
    lastProposer = m.from
    updateLastEvent()

    if (committed) {
      // Ensure the proposer knows we've successfully committed
      val txc = TxCommitted(lastProposer, storeId, transactionId, objectCommitErrors)
      net.sendTransactionMessage(txc)
    }

    val result = acceptor.receivePrepare(Prepare(m.proposalId)) match {
      case Right(p) => Right(TxPrepareResponse.Promise(p.lastAccepted))
      case Left(n) => Left(TxPrepareResponse.Nack(n.promisedProposalId))
    }

    val txr = TxPrepareResponse(m.from, storeId, transactionId, result, m.proposalId, disposition, collisions)

    delayedPrepare.response = Some(txr)

    if (allObjectsLoaded) {
      delayedPrepare.saveId = nextCrlSaveId()
      val state = TransactionRecoveryState(storeId, serializedTxd, trsObjectUpdates,
        disposition, status, acceptor.persistentState)
      crl.save(txd.transactionId, state, delayedPrepare.saveId)
    }
  }

  def receiveAccept(m: TxAccept): Unit = {
    updateLastEvent()

    val result = acceptor.receiveAccept(Accept(m.proposalId, m.value)) match {
      case Right(a) => Right(TxAcceptResponse.Accepted(a.proposalValue))
      case Left(n) => Left(TxAcceptResponse.Nack(n.promisedProposalId))
    }
    delayedAccept.response = Some(TxAcceptResponse(m.from, storeId, transactionId, m.proposalId, result))
    delayedAccept.saveId = nextCrlSaveId()
    val state = TransactionRecoveryState(storeId, serializedTxd, trsObjectUpdates,
      disposition, status, acceptor.persistentState)
    crl.save(txd.transactionId, state, delayedAccept.saveId)
  }

  def receiveResolved(m: TxResolved): Unit = {
    updateLastEvent()
    resolved(m.committed)
  }

  def receiveFinalized(m: TxFinalized): Unit = if (ofinalized.isEmpty) {
    updateLastEvent()
    resolved(m.committed)
    ofinalized = Some(m.committed)
    if (m.committed)
      crl.deleteTransaction(storeId, transactionId)
    statusCache.updateStatus(transactionId, status, finalized = true)
  }

  def receiveHeartbeat(m: TxHeartbeat): Unit = {
    updateLastEvent()
  }

  def receiveStatusRequest(m: TxStatusRequest): Unit = {
    val r = TxStatusResponse(m.from, storeId, transactionId, m.requestUUID,
      Some(TxStatusResponse.TxStatus(status, ofinalized.nonEmpty)))
    net.sendTransactionMessage(r)
  }

  def crlSaveComplete(saveId: TxSaveId): Unit = {
    if (saveId == delayedPrepare.saveId) {
      delayedPrepare.response.foreach { m =>
        net.sendTransactionMessage(m)
        delayedPrepare.response = None
      }
    }

    if (saveId == delayedAccept.saveId) {
      delayedAccept.response.foreach { m =>
        net.sendTransactionMessage(m)
        delayedAccept.response = None
      }
    }
  }

  def commitComplete(objectId: ObjectId, result: Either[Unit, CommitError.Value]): Unit = {
    pendingObjectCommits -= 1
    if (pendingObjectCommits == 0) {
      committed = true
      saveObjectUpdates = false
      crl.dropTransactionObjectData(storeId, transactionId)
      val m = TxCommitted(lastProposer, storeId, transactionId, objectCommitErrors)
      net.sendTransactionMessage(m)
    }
  }
}
