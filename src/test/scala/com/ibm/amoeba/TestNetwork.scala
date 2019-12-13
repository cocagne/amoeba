package com.ibm.amoeba

import java.util.UUID

import com.ibm.amoeba.common.network.{ClientResponse, TxMessage}
import com.ibm.amoeba.common.objects.ObjectId
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{TransactionDescription, TransactionId}
import com.ibm.amoeba.server.StoreManager
import com.ibm.amoeba.server.crl.{AllocSaveComplete, AllocationRecoveryState, CrashRecoveryLog, CrashRecoveryLogClient, CrashRecoveryLogFactory, SaveCompletionHandler, TransactionRecoveryState, TxSaveComplete, TxSaveId}
import com.ibm.amoeba.server.network.Messenger
import com.ibm.amoeba.server.store.backend.MapBackend
import com.ibm.amoeba.server.store.cache.SimpleLRUObjectCache
import com.ibm.amoeba.server.transaction.{TransactionDriver, TransactionFinalizer}

import scala.concurrent.Future


object TestNetwork {
  val crlClientId = CrashRecoveryLogClient(0)

  class TestCRL(val completionHandler: SaveCompletionHandler) extends CrashRecoveryLog {
    override def getFullRecoveryState(storeId: StoreId): (List[TransactionRecoveryState], List[AllocationRecoveryState]) = (Nil, Nil)

    override def save(txid: TransactionId, state: TransactionRecoveryState, saveId: TxSaveId): Unit = {
      completionHandler.saveComplete(TxSaveComplete(crlClientId, state.storeId, txid, saveId))
    }

    override def save(state: AllocationRecoveryState): Unit = {
      completionHandler.saveComplete(AllocSaveComplete(crlClientId, state.allocationTransactionId, state.storeId, state.newObjectId))
    }

    override def dropTransactionObjectData(storeId: StoreId, txid: TransactionId): Unit = ()

    override def deleteTransaction(storeId: StoreId, txid: TransactionId): Unit = ()

    override def deleteAllocation(storeId: StoreId, txid: TransactionId): Unit = ()
  }

  object TestCRL extends CrashRecoveryLogFactory {
    override def createCRL(completionHandler: SaveCompletionHandler): CrashRecoveryLog = new TestCRL(completionHandler)
  }

  class NullFinalizer extends TransactionFinalizer {
    override def complete: Future[Unit] = Future.successful(())

    override def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = ()


    override def debugStatus: List[(String, Boolean)] = Nil

    override def cancel(): Unit = ()
  }

  object NullFinalizer extends TransactionFinalizer.Factory {
    override def create(txd: TransactionDescription, messenger: Messenger): TransactionFinalizer = new NullFinalizer
  }
}


class TestNetwork extends Messenger {
  import TestNetwork._

  val objectCache = new SimpleLRUObjectCache(1000)

  val poolId = PoolId(new UUID(0, 0))

  val storeId0 = StoreId(poolId, 0)
  val storeId1 = StoreId(poolId, 1)
  val storeId2 = StoreId(poolId, 2)

  val store0 = new MapBackend(storeId0)
  val store1 = new MapBackend(storeId1)
  val store2 = new MapBackend(storeId2)

  val smgr = new StoreManager(objectCache, this, TestCRL, NullFinalizer, TransactionDriver.noErrorRecoveryFactory,
    List(store0, store1, store2))

  // process load store events
  smgr.handleEvents()

  private var topSend = true

  override def sendClientResponse(msg: ClientResponse): Unit = ()

  override def sendTransactionMessage(msg: TxMessage): Unit = {
    val isTopSend = topSend

    if (isTopSend)
      topSend = false

    smgr.receiveTransactionMessage(msg)

    if (isTopSend) {
      while (smgr.hasEvents)
        smgr.handleEvents()
      topSend = true
    }
  }

  override def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(sendTransactionMessage)
}
