package com.ibm.amoeba.server.crl.simple

import com.ibm.amoeba.FileBasedTests
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.objects.{ObjectId, ObjectRefcount, ObjectType}
import com.ibm.amoeba.common.paxos.{PersistentState, ProposalId}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.{ObjectUpdate, TransactionDisposition, TransactionId, TransactionStatus}
import com.ibm.amoeba.server.crl.{AllocationRecoveryState, TransactionRecoveryState}

import java.nio.ByteBuffer
import java.util.UUID

object SimpleCRLSuite {
  val transactionId = TransactionId(new UUID(0, 1))
  val poolId = PoolId(new UUID(0, 2))
  val storeId = StoreId(poolId, 1)

  val txid = TxId(storeId, transactionId)

  val txd = DataBuffer(Array[Byte](1, 2))
  val oud1 = DataBuffer(Array[Byte](3, 4))
  val oud2 = DataBuffer(Array[Byte](4))
  val ou1 = ObjectUpdate(ObjectId(new UUID(0, 3)), oud1)
  val ou2 = ObjectUpdate(ObjectId(new UUID(0, 4)), oud2)
  val disp = TransactionDisposition.VoteCommit
  val status = TransactionStatus.Unresolved
  val promise = ProposalId(1, 1)
  val accept = ProposalId(2, 2)
  val pax = PersistentState(Some(promise), Some((accept, true)))
  val trs = TransactionRecoveryState(storeId, txd, List(ou1, ou2), disp, status, pax)

  val storePointerEmpty = StorePointer(storeId.poolIndex, Array[Byte]())
  val storePointerData = StorePointer(storeId.poolIndex, Array[Byte](1,2,3))
  val objectId = ObjectId(new UUID(0,5))
  val objectData = DataBuffer(Array[Byte](0,1))
  val refcount = ObjectRefcount(1,1)
  val timestamp = HLCTimestamp(2)
  val allocTxId = TransactionId(new UUID(0,6))
  val serializedRevisionGuard = DataBuffer(Array[Byte](0,1,2,3,4))
  val allocDataLocation = StreamLocation(StreamId(0), 5, 4)

  val txdLoc = StreamLocation(StreamId(0), 16, 4)
  val ou1Loc = StreamLocation(StreamId(0), 25, 2)
  val ou2Loc = StreamLocation(StreamId(0), 27, 1)

  val updateLocations = Some((ou1.objectId, ou1Loc) :: (ou2.objectId, ou2Loc) :: Nil)
}

class SimpleCRLSuite extends FileBasedTests {
  import SimpleCRLSuite._

  test("Alloc Static Save & Load With Empty StorePointer & Data") {
    val ars = AllocationRecoveryState( storeId, storePointerEmpty, objectId, ObjectType.KeyValue,
      None, objectData, refcount, timestamp, allocTxId, serializedRevisionGuard)

    val a = Alloc(Some(allocDataLocation), ars)

    val buffer = new Array[Byte](4096 * 5)

    assert(a.dynamicDataSize == 0)

    a.writeStaticEntry(ByteBuffer.wrap(buffer))

    val la = Alloc.loadAlloc(ByteBuffer.wrap(buffer))

    assert(la.txid.storeId == ars.storeId)
    assert(la.txid.transactionId == ars.allocationTransactionId)
    assert(la.storePointer == ars.storePointer)
    assert(la.newObjectId == ars.newObjectId)
    assert(la.objectType == ars.objectType)
    assert(la.objectSize == ars.objectSize)
    assert(la.initialRefcount == ars.initialRefcount)
    assert(la.timestamp == ars.timestamp)
    assert(la.serializedRevisionGuard == ars.serializedRevisionGuard)
  }

  test("Alloc Static Save & Load With NonEmpty StorePointer & No Data") {
    val noObjectData = DataBuffer(Array[Byte]())
    val ars = AllocationRecoveryState(storeId, storePointerData, objectId, ObjectType.Data,
      None, noObjectData, refcount, timestamp, allocTxId, serializedRevisionGuard)

    val a = Alloc(Some(allocDataLocation), ars)

    val buffer = new Array[Byte](4096 * 5)

    assert(a.dynamicDataSize == 0)

    a.writeStaticEntry(ByteBuffer.wrap(buffer))

    val la = Alloc.loadAlloc(ByteBuffer.wrap(buffer))

    assert(la.txid.storeId == ars.storeId)
    assert(la.txid.transactionId == ars.allocationTransactionId)
    assert(la.storePointer == ars.storePointer)
    assert(la.newObjectId == ars.newObjectId)
    assert(la.objectType == ars.objectType)
    assert(la.objectSize == ars.objectSize)
    assert(la.initialRefcount == ars.initialRefcount)
    assert(la.timestamp == ars.timestamp)
    assert(la.serializedRevisionGuard == ars.serializedRevisionGuard)
  }

  test("Tx Static Save & Load with ObjectUpdate data") {
    val tx = Tx(txid, trs, Some(txdLoc), updateLocations)

    val buffer = new Array[Byte](4096 * 5)

    assert(tx.dynamicDataSize == 0)

    tx.writeStaticEntry(ByteBuffer.wrap(buffer))

    val ltx = Tx.loadTx(ByteBuffer.wrap(buffer))

    assert(ltx.id == txid)
    assert(ltx.disposition == disp)
    assert(ltx.txdLocation == txdLoc)
    assert(ltx.updateLocations == updateLocations)
    assert(ltx.paxosAcceptorState == pax)
  }

  test("Tx Static Save & Load without ObjectUpdate data") {
    val tx = Tx(txid, trs, Some(txdLoc), None, keepObjectUpdates = false)

    val buffer = new Array[Byte](4096 * 5)

    assert(tx.dynamicDataSize == 0)

    tx.writeStaticEntry(ByteBuffer.wrap(buffer))

    val ltx = Tx.loadTx(ByteBuffer.wrap(buffer))

    assert(ltx.id == txid)
    assert(ltx.disposition == disp)
    assert(ltx.txdLocation == txdLoc)
    assert(ltx.updateLocations.isEmpty)
    assert(ltx.paxosAcceptorState == pax)
  }
}
