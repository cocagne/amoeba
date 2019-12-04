package com.ibm.amoeba.store

import java.util.UUID

import com.ibm.amoeba.common.ida.Replication
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.network.{Allocate, AllocateResponse, ClientId, ClientResponse, TxMessage, TxPrepare, TxResolved}
import com.ibm.amoeba.common.objects.{DataObjectPointer, ObjectId, ObjectRefcount, ObjectRevision, ObjectRevisionGuard, ObjectType}
import com.ibm.amoeba.common.paxos.ProposalId
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionDescription, TransactionId}
import com.ibm.amoeba.server.crl.{AllocSaveComplete, AllocationRecoveryState, CrashRecoveryLog, CrashRecoveryLogClient, TransactionRecoveryState, TxSaveId}
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.store.{Frontend, TransactionStatusCache}
import com.ibm.amoeba.server.store.backend.MapBackend
import com.ibm.amoeba.server.store.cache.SimpleLRUObjectCache
import org.scalatest.{FunSuite, Matchers}

object FrontendSuite {

  val poolId = PoolId(new UUID(0,1))
  val storeId = StoreId(poolId, 0)
  val oid1 = ObjectId(new UUID(0,2))
  val oid2 = ObjectId(new UUID(0,3))
  val clientId = ClientId(new UUID(0,4))
  val txid1 = TransactionId(new UUID(0,5))
  val txid2 = TransactionId(new UUID(0,6))
  val rev0 = new ObjectRevision(new UUID(0, 7))
  val sp1 = StorePointer(0, Array[Byte]())
  val op1 = new DataObjectPointer(oid1, poolId, None, Replication(3,2), Array(sp1))
  val crlClient = CrashRecoveryLogClient(1)

  class TestNet extends Messenger {

    var cr: Option[ClientResponse] = None
    var tx: Option[TxMessage] = None

    override def sendClientResponse(msg: ClientResponse): Unit = cr = Some(msg)

    override def sendTransactionMessage(msg: TxMessage): Unit = tx = Some(msg)

    def clientMessage(): Option[ClientResponse] = {
      val t = cr
      cr = None
      t
    }

    def txMessage(): Option[TxMessage] = {
      val t = tx
      tx = None
      t
    }
  }

  class TestCrl extends CrashRecoveryLog {

    var txSaved = false
    var aSaved = false
    var txDel = false
    var aDel = false
    var aDrop = false

    override def getFullRecoveryState(storeId: StoreId): (List[TransactionRecoveryState], List[AllocationRecoveryState]) = (Nil, Nil)

    override def save(txid: TransactionId, state: TransactionRecoveryState, saveId: TxSaveId): Unit = txSaved = true

    override def save(state: AllocationRecoveryState): Unit = aSaved = true

    override def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit = aDrop = true

    override def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = txDel = true

    override def deleteAllocation(storeId: StoreId, txid: TransactionId): Unit = aDel = true
  }
}

class FrontendSuite extends FunSuite with Matchers {
  import FrontendSuite._

  test("Allocation") {
    val backend = new MapBackend(storeId)
    val cache = new SimpleLRUObjectCache(100)
    val net = new TestNet
    val crl = new TestCrl
    val iref = ObjectRefcount(1,1)

    val idata = DataBuffer(Array[Byte](0,1))
    val its = HLCTimestamp(1)


    val f = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache())

    val ma = Allocate( toStore = storeId,
      fromClient = clientId,
      newObjectId = oid1,
      objectType = ObjectType.Data,
      objectSize = None,
      initialRefcount = iref,
      objectData = idata,
      timestamp = its,
      allocationTransactionId = txid1,
      revisionGuard = ObjectRevisionGuard(op1, rev0)
    )

    assert(backend.get(oid1).isEmpty)

    f.allocateObject(ma)

    assert(crl.aSaved)

    assert(cache.get(oid1).nonEmpty)
    assert(backend.get(oid1).isEmpty)

    f.crlSaveComplete(AllocSaveComplete(crlClient, txid1, storeId, oid1))

    assert(net.cr.nonEmpty)

    val ar = net.cr.get.asInstanceOf[AllocateResponse]

    assert(ar.toClient == clientId)
    assert(ar.fromStore == storeId)
    assert(ar.allocationTransactionId == txid1)
    assert(ar.newObjectId == oid1)
    assert(ar.result.contains(sp1))

    assert(cache.get(oid1).nonEmpty)
    assert(backend.get(oid1).isEmpty)

    f.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, true))

    assert(backend.get(oid1).nonEmpty)
  }

  test("Update Allocating Object") {
    val backend = new MapBackend(storeId)
    val cache = new SimpleLRUObjectCache(100)
    val net = new TestNet
    val crl = new TestCrl
    val iref = ObjectRefcount(1,1)

    val idata = DataBuffer(Array[Byte](0,1))
    val its = HLCTimestamp(1)


    val f = new Frontend(storeId, backend, cache, net, crl, new TransactionStatusCache())

    val ma = Allocate( toStore = storeId,
      fromClient = clientId,
      newObjectId = oid1,
      objectType = ObjectType.Data,
      objectSize = None,
      initialRefcount = iref,
      objectData = idata,
      timestamp = its,
      allocationTransactionId = txid1,
      revisionGuard = ObjectRevisionGuard(op1, rev0)
    )

    assert(backend.get(oid1).isEmpty)

    f.allocateObject(ma)

    f.crlSaveComplete(AllocSaveComplete(crlClient, txid1, storeId, oid1))

    assert(backend.get(oid1).isEmpty)

    val txd = TransactionDescription(txid1, its, op1, 1.asInstanceOf[Byte],
      List(DataUpdate(op1, ObjectRevision(txid1), DataUpdateOperation.Overwrite)),
      List(), None, List(), List())

    val db = DataBuffer(Array[Byte](0, 1, 2, 3))
    val ou = ObjectUpdate(oid1.uuid, db)
    val prep = TxPrepare(storeId, storeId, txd, ProposalId.initialProposal(1), List(ou), List())

    f.receiveTransactionMessage(prep)

    f.receiveTransactionMessage(TxResolved(storeId, storeId, txid1, true))

    assert(backend.get(oid1).nonEmpty)

    val os = backend.get(oid1).get

    assert(os.data.size == db.size)
  }
}
