package com.ibm.amoeba.server.crl.sweeper

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue

import com.ibm.amoeba.FileBasedTests
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.objects.{ObjectId, ObjectRefcount, ObjectType}
import com.ibm.amoeba.common.paxos.{PersistentState, ProposalId}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.{ObjectUpdate, TransactionDisposition, TransactionId, TransactionStatus}
import com.ibm.amoeba.server.crl.{AllocationRecoveryState, CrashRecoveryLog, SaveCompletion, SaveCompletionHandler, TransactionRecoveryState, TxSaveId}


object SweeperSuite {
  class CompletionHandler extends SaveCompletionHandler {
    val completions = new LinkedBlockingQueue[SaveCompletion]

    def saveComplete(op: SaveCompletion): Unit = {
      completions.put(op)
    }
  }
}

class SweeperSuite extends FileBasedTests {

  import SweeperSuite._

  test("Simple Save") {

    val h = new CompletionHandler

    val txid = TransactionId(new UUID(0, 1))
    val poolId = PoolId(new UUID(0, 2))

    val storeId = StoreId(poolId, 1)
    val saveId = TxSaveId(1)
    val txd = DataBuffer(Array[Byte](1, 2))
    val oud1 = DataBuffer(Array[Byte](3,4))
    val oud2 = DataBuffer(Array[Byte](4))
    val ou1 = ObjectUpdate(ObjectId(new UUID(0,3)), oud1)
    val ou2 = ObjectUpdate(ObjectId(new UUID(0,4)), oud2)
    val disp = TransactionDisposition.VoteCommit
    val status = TransactionStatus.Unresolved
    val promise = ProposalId(1, 1)
    val accept = ProposalId(2, 2)
    val pax = PersistentState(Some(promise), Some((accept, true)))
    val trs = TransactionRecoveryState(storeId, txd, List(ou1, ou2), disp, status, pax)

    val s = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s.createCRL(h)

      crl.save(txid, trs, saveId)

      h.completions.take()

    } finally {
      e: Throwable =>
        s.shutdown()
        throw e
    }

    val s2 = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s2.createCRL(h)

      val (txs, alloc) = crl.getFullRecoveryState(storeId)

      assert(txs.length == 1)

      val t = txs.head

      assert(t.storeId == storeId)
      assert(t.serializedTxd.asReadOnlyBuffer() == txd.asReadOnlyBuffer())
      assert(t.objectUpdates.length == 2)
      assert(t.objectUpdates.head.objectId == ou2.objectId)
      assert(t.objectUpdates.head.data.asReadOnlyBuffer() == ou2.data.asReadOnlyBuffer())
      assert(t.objectUpdates.tail.head.objectId == ou1.objectId)
      assert(t.objectUpdates.tail.head.data.asReadOnlyBuffer() == ou1.data.asReadOnlyBuffer())
      assert(t.disposition == disp)
      assert(t.status == status)
      assert(t.paxosAcceptorState == pax)

    } finally {
      e: Throwable =>
        s2.shutdown()
        throw e
    }
  }


  test("Simple Tx and Alloc Save") {

    val h = new CompletionHandler

    val txid = TransactionId(new UUID(0, 1))
    val poolId = PoolId(new UUID(0, 2))

    val storeId = StoreId(poolId, 1)
    val saveId = TxSaveId(1)
    val txd = DataBuffer(Array[Byte](1, 2))
    val oud1 = DataBuffer(Array[Byte](3,4))
    val oud2 = DataBuffer(Array[Byte](4))
    val ou1 = ObjectUpdate(ObjectId(new UUID(0,3)), oud1)
    val ou2 = ObjectUpdate(ObjectId(new UUID(0,4)), oud2)
    val disp = TransactionDisposition.VoteCommit
    val status = TransactionStatus.Unresolved
    val promise = ProposalId(1, 1)
    val accept = ProposalId(2, 2)
    val pax = PersistentState(Some(promise), Some((accept, true)))
    val trs = TransactionRecoveryState(storeId, txd, List(ou1, ou2), disp, status, pax)

    val sp = StorePointer(1, Array[Byte](1,2))
    val oid = ObjectId(new UUID(1,1))
    val otype = ObjectType.Data
    val ref = ObjectRefcount(1,1)
    val ts = HLCTimestamp(5)
    val ars = AllocationRecoveryState(storeId, sp, oid, otype, None, txd, ref, ts, txid, oud1)

    val s = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s.createCRL(h)

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(ars)

      h.completions.take()

    } finally {
      e: Throwable =>
        s.shutdown()
        throw e
    }

    val s2 = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s2.createCRL(h)

      val (txs, alloc) = crl.getFullRecoveryState(storeId)

      assert(txs.length == 1)

      val t = txs.head

      assert(t.storeId == storeId)
      assert(t.serializedTxd.asReadOnlyBuffer() == txd.asReadOnlyBuffer())
      assert(t.objectUpdates.length == 2)
      assert(t.objectUpdates.head.objectId == ou2.objectId)
      assert(t.objectUpdates.head.data.asReadOnlyBuffer() == ou2.data.asReadOnlyBuffer())
      assert(t.objectUpdates.tail.head.objectId == ou1.objectId)
      assert(t.objectUpdates.tail.head.data.asReadOnlyBuffer() == ou1.data.asReadOnlyBuffer())
      assert(t.disposition == disp)
      assert(t.status == status)
      assert(t.paxosAcceptorState == pax)

      assert(alloc.length == 1)

      val a = alloc.head

      assert(a.storeId == storeId)
      assert(a.storePointer.poolIndex == sp.poolIndex)
      assert(ByteBuffer.wrap(a.storePointer.data) == ByteBuffer.wrap(sp.data))
      assert(a.newObjectId == oid)
      assert(a.objectType == otype)
      assert(a.initialRefcount == ref)
      assert(a.timestamp == ts)
      assert(a.allocationTransactionId == txid)
      assert(a.serializedRevisionGuard == oud1)

    } finally {
      e: Throwable =>
        s2.shutdown()
        throw e
    }
  }

  test("Simple Tx and Alloc Save with Multiple Streams") {

    val h = new CompletionHandler

    val txid = TransactionId(new UUID(0, 1))
    val poolId = PoolId(new UUID(0, 2))

    val storeId = StoreId(poolId, 1)
    val saveId = TxSaveId(1)
    val txd = DataBuffer(Array[Byte](1, 2))
    val oud1 = DataBuffer(Array[Byte](3,4))
    val oud2 = DataBuffer(Array[Byte](4))
    val ou1 = ObjectUpdate(ObjectId(new UUID(0,3)), oud1)
    val ou2 = ObjectUpdate(ObjectId(new UUID(0,4)), oud2)
    val disp = TransactionDisposition.VoteCommit
    val status = TransactionStatus.Unresolved
    val promise = ProposalId(1, 1)
    val accept = ProposalId(2, 2)
    val pax = PersistentState(Some(promise), Some((accept, true)))
    val trs = TransactionRecoveryState(storeId, txd, List(ou1, ou2), disp, status, pax)

    val sp = StorePointer(1, Array[Byte](1,2))
    val oid = ObjectId(new UUID(1,1))
    val otype = ObjectType.Data
    val ref = ObjectRefcount(1,1)
    val ts = HLCTimestamp(5)
    val ars = AllocationRecoveryState(storeId, sp, oid, otype, None, txd, ref, ts, txid, oud1)

    val s = new Sweeper(tdir.toPath, 3, 4096*3, 5)
    try {
      val crl = s.createCRL(h)

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(ars)

      h.completions.take()

    } finally {
      e: Throwable =>
        s.shutdown()
        throw e
    }

    val s2 = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s2.createCRL(h)

      val (txs, alloc) = crl.getFullRecoveryState(storeId)

      assert(txs.length == 1)

      val t = txs.head

      assert(t.storeId == storeId)
      assert(t.serializedTxd.asReadOnlyBuffer() == txd.asReadOnlyBuffer())
      assert(t.objectUpdates.length == 2)
      assert(t.objectUpdates.head.objectId == ou2.objectId)
      assert(t.objectUpdates.head.data.asReadOnlyBuffer() == ou2.data.asReadOnlyBuffer())
      assert(t.objectUpdates.tail.head.objectId == ou1.objectId)
      assert(t.objectUpdates.tail.head.data.asReadOnlyBuffer() == ou1.data.asReadOnlyBuffer())
      assert(t.disposition == disp)
      assert(t.status == status)
      assert(t.paxosAcceptorState == pax)

      assert(alloc.length == 1)

      val a = alloc.head

      assert(a.storeId == storeId)
      assert(a.storePointer.poolIndex == sp.poolIndex)
      assert(ByteBuffer.wrap(a.storePointer.data) == ByteBuffer.wrap(sp.data))
      assert(a.newObjectId == oid)
      assert(a.objectType == otype)
      assert(a.initialRefcount == ref)
      assert(a.timestamp == ts)
      assert(a.allocationTransactionId == txid)
      assert(a.serializedRevisionGuard == oud1)

    } finally {
      e: Throwable =>
        s2.shutdown()
        throw e
    }
  }


  test("Overwrite State") {

    val h = new CompletionHandler

    val txid = TransactionId(new UUID(0, 1))
    val poolId = PoolId(new UUID(0, 2))

    val storeId = StoreId(poolId, 1)
    val saveId = TxSaveId(1)
    val txd = DataBuffer(Array[Byte](1, 2))
    val oud1 = DataBuffer(Array[Byte](3,4))
    val oud2 = DataBuffer(Array[Byte](4))
    val ou1 = ObjectUpdate(ObjectId(new UUID(0,3)), oud1)
    val ou2 = ObjectUpdate(ObjectId(new UUID(0,4)), oud2)
    val disp = TransactionDisposition.VoteCommit
    val disp2 = TransactionDisposition.VoteAbort
    val status = TransactionStatus.Unresolved
    val promise = ProposalId(1, 1)
    val accept = ProposalId(2, 2)
    val pax = PersistentState(Some(promise), Some((accept, true)))
    val trs = TransactionRecoveryState(storeId, txd, List(ou1, ou2), disp, status, pax)
    val trs2 = trs.copy(disposition = disp2)

    val s = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s.createCRL(h)

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid, trs2, saveId)

      h.completions.take()

    } finally {
      e: Throwable =>
        s.shutdown()
        throw e
    }

    val s2 = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s2.createCRL(h)

      val (txs, alloc) = crl.getFullRecoveryState(storeId)

      assert(txs.length == 1)

      val t = txs.head

      assert(t.storeId == storeId)
      assert(t.serializedTxd.asReadOnlyBuffer() == txd.asReadOnlyBuffer())
      assert(t.objectUpdates.length == 2)
      assert(t.objectUpdates.head.objectId == ou2.objectId)
      assert(t.objectUpdates.head.data.asReadOnlyBuffer() == ou2.data.asReadOnlyBuffer())
      assert(t.objectUpdates.tail.head.objectId == ou1.objectId)
      assert(t.objectUpdates.tail.head.data.asReadOnlyBuffer() == ou1.data.asReadOnlyBuffer())
      assert(t.disposition == disp2)
      assert(t.status == status)
      assert(t.paxosAcceptorState == pax)

    } finally {
      e: Throwable =>
        s2.shutdown()
        throw e
    }
  }

  test("Delete State") {

    val h = new CompletionHandler

    val txid = TransactionId(new UUID(0, 1))
    val txid2 = TransactionId(new UUID(0, 3))
    val poolId = PoolId(new UUID(0, 2))

    val storeId = StoreId(poolId, 1)
    val saveId = TxSaveId(1)
    val txd = DataBuffer(Array[Byte](1, 2))
    val oud1 = DataBuffer(Array[Byte](3,4))
    val oud2 = DataBuffer(Array[Byte](4))
    val ou1 = ObjectUpdate(ObjectId(new UUID(0,3)), oud1)
    val ou2 = ObjectUpdate(ObjectId(new UUID(0,4)), oud2)
    val disp = TransactionDisposition.VoteCommit
    val disp2 = TransactionDisposition.VoteAbort
    val status = TransactionStatus.Unresolved
    val promise = ProposalId(1, 1)
    val accept = ProposalId(2, 2)
    val pax = PersistentState(Some(promise), Some((accept, true)))
    val trs = TransactionRecoveryState(storeId, txd, List(ou1, ou2), disp, status, pax)
    val trs2 = trs.copy(disposition = disp2)

    val s = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s.createCRL(h)

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid2, trs, saveId)

      h.completions.take()

      crl.deleteTransaction(storeId, txid)

      crl.save(txid2, trs2, saveId)

      h.completions.take()

    } finally {
      e: Throwable =>
        s.shutdown()
        throw e
    }

    val s2 = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s2.createCRL(h)

      val (txs, alloc) = crl.getFullRecoveryState(storeId)

      assert(txs.length == 1)

      val t = txs.head

      assert(t.storeId == storeId)
      assert(t.serializedTxd.asReadOnlyBuffer() == txd.asReadOnlyBuffer())
      assert(t.objectUpdates.length == 2)
      assert(t.objectUpdates.head.objectId == ou2.objectId)
      assert(t.objectUpdates.head.data.asReadOnlyBuffer() == ou2.data.asReadOnlyBuffer())
      assert(t.objectUpdates.tail.head.objectId == ou1.objectId)
      assert(t.objectUpdates.tail.head.data.asReadOnlyBuffer() == ou1.data.asReadOnlyBuffer())
      assert(t.disposition == disp2)
      assert(t.status == status)
      assert(t.paxosAcceptorState == pax)

    } finally {
      e: Throwable =>
        s2.shutdown()
        throw e
    }
  }

  test("File Rotation") {

    val h = new CompletionHandler

    val txid = TransactionId(new UUID(0, 1))
    val poolId = PoolId(new UUID(0, 2))

    val storeId = StoreId(poolId, 1)
    val saveId = TxSaveId(1)
    val txd = DataBuffer(Array[Byte](1, 2))
    val oud1 = DataBuffer(Array[Byte](3,4))
    val oud2 = DataBuffer(Array[Byte](4))
    val ou1 = ObjectUpdate(ObjectId(new UUID(0,3)), oud1)
    val ou2 = ObjectUpdate(ObjectId(new UUID(0,4)), oud2)
    val disp = TransactionDisposition.VoteCommit
    val disp2 = TransactionDisposition.VoteAbort
    val status = TransactionStatus.Unresolved
    val promise = ProposalId(1, 1)
    val accept = ProposalId(2, 2)
    val pax = PersistentState(Some(promise), Some((accept, true)))
    val trs = TransactionRecoveryState(storeId, txd, List(ou1, ou2), disp, status, pax)
    val trs2 = trs.copy(disposition = disp2)

    val s = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s.createCRL(h)

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid, trs2, saveId)

      h.completions.take()

    } finally {
      e: Throwable =>
        s.shutdown()
        throw e
    }

    val s2 = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s2.createCRL(h)

      val (txs, alloc) = crl.getFullRecoveryState(storeId)

      assert(txs.length == 1)

      val t = txs.head

      assert(t.storeId == storeId)
      assert(t.serializedTxd.asReadOnlyBuffer() == txd.asReadOnlyBuffer())
      assert(t.objectUpdates.length == 2)
      assert(t.objectUpdates.head.objectId == ou2.objectId)
      assert(t.objectUpdates.head.data.asReadOnlyBuffer() == ou2.data.asReadOnlyBuffer())
      assert(t.objectUpdates.tail.head.objectId == ou1.objectId)
      assert(t.objectUpdates.tail.head.data.asReadOnlyBuffer() == ou1.data.asReadOnlyBuffer())
      assert(t.disposition == disp2)
      assert(t.status == status)
      assert(t.paxosAcceptorState == pax)

    } finally {
      e: Throwable =>
        s2.shutdown()
        throw e
    }
  }

  test("Wrapping File Rotation") {

    val h = new CompletionHandler

    val txid = TransactionId(new UUID(0, 1))
    val poolId = PoolId(new UUID(0, 2))

    val storeId = StoreId(poolId, 1)
    val saveId = TxSaveId(1)
    val txd = DataBuffer(Array[Byte](1, 2))
    val oud1 = DataBuffer(Array[Byte](3,4))
    val oud2 = DataBuffer(Array[Byte](4))
    val ou1 = ObjectUpdate(ObjectId(new UUID(0,3)), oud1)
    val ou2 = ObjectUpdate(ObjectId(new UUID(0,4)), oud2)
    val disp = TransactionDisposition.VoteCommit
    val disp2 = TransactionDisposition.VoteAbort
    val status = TransactionStatus.Unresolved
    val promise = ProposalId(1, 1)
    val accept = ProposalId(2, 2)
    val pax = PersistentState(Some(promise), Some((accept, true)))
    val trs = TransactionRecoveryState(storeId, txd, List(ou1, ou2), disp, status, pax)
    val trs2 = trs.copy(disposition = disp2)

    val s = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s.createCRL(h)

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid, trs, saveId)

      h.completions.take()

      crl.save(txid, trs2, saveId)

      h.completions.take()

    } finally {
      e: Throwable =>
        s.shutdown()
        throw e
    }

    val s2 = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s2.createCRL(h)

      val (txs, alloc) = crl.getFullRecoveryState(storeId)

      assert(txs.length == 1)

      val t = txs.head

      assert(t.storeId == storeId)
      assert(t.serializedTxd.asReadOnlyBuffer() == txd.asReadOnlyBuffer())
      assert(t.objectUpdates.length == 2)
      assert(t.objectUpdates.head.objectId == ou2.objectId)
      assert(t.objectUpdates.head.data.asReadOnlyBuffer() == ou2.data.asReadOnlyBuffer())
      assert(t.objectUpdates.tail.head.objectId == ou1.objectId)
      assert(t.objectUpdates.tail.head.data.asReadOnlyBuffer() == ou1.data.asReadOnlyBuffer())
      assert(t.disposition == disp2)
      assert(t.status == status)
      assert(t.paxosAcceptorState == pax)

    } finally {
      e: Throwable =>
        s2.shutdown()
        throw e
    }
  }
}