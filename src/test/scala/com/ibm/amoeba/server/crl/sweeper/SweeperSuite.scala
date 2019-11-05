package com.ibm.amoeba.server.crl.sweeper

import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue

import com.ibm.amoeba.FileBasedTests
import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.paxos.{PersistentState, ProposalId}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{ObjectUpdate, TransactionDisposition, TransactionId, TransactionStatus}
import com.ibm.amoeba.server.crl.{CrashRecoveryLog, SaveCompletion, SaveCompletionHandler, TransactionRecoveryState, TxSaveId}


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

  test("Simple Save and Delete") {

    val h = new CompletionHandler

    val txid = TransactionId(new UUID(0, 1))
    val poolId = PoolId(new UUID(0, 2))

    val storeId = StoreId(poolId, 1)
    val saveId = TxSaveId(1)
    val txd = DataBuffer(Array[Byte](1, 2))
    val oud1 = DataBuffer(Array[Byte](3,4))
    val oud2 = DataBuffer(Array[Byte](4))
    val ou1 = ObjectUpdate(new UUID(0,3), oud1)
    val ou2 = ObjectUpdate(new UUID(0,4), oud2)
    val disp = TransactionDisposition.VoteCommit
    val status = TransactionStatus.Unresolved
    val promise = ProposalId(1, 1)
    val accept = ProposalId(2, 2)
    val pax = PersistentState(Some(promise), Some((accept, true)))
    val trs = TransactionRecoveryState(storeId, txd, List(ou1, ou2), disp, status, pax)

    val s = new Sweeper(tdir.toPath, 1, 4096*3, 5)
    try {
      val crl = s.createCRL(h)

      println("Saving CRL state")
      crl.save(txid, trs, saveId)
      println("Awaiting Completion of save")
      h.completions.take()
      println("SAve complete")

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

    } finally {
      e: Throwable =>
        s2.shutdown()
        throw e
    }
  }
}