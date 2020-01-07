package com.ibm.amoeba

import java.util.UUID

import com.ibm.amoeba.client.internal.OpportunisticRebuildManager
import com.ibm.amoeba.client.{AmoebaClient, ObjectCache, TransactionStatusCache}
import com.ibm.amoeba.client.internal.network.{Messenger => ClientMessenger}
import com.ibm.amoeba.common.ida.Replication
import com.ibm.amoeba.common.network.{ClientId, ClientRequest, ClientResponse, TxMessage}
import com.ibm.amoeba.common.objects.{KeyValueObjectPointer, ObjectId}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{TransactionDescription, TransactionId}
import com.ibm.amoeba.common.util.BackgroundTask
import com.ibm.amoeba.server.StoreManager
import com.ibm.amoeba.server.crl.{AllocSaveComplete, AllocationRecoveryState, CrashRecoveryLog, CrashRecoveryLogClient, CrashRecoveryLogFactory, SaveCompletionHandler, TransactionRecoveryState, TxSaveComplete, TxSaveId}
import com.ibm.amoeba.server.network.{Messenger => ServerMessenger}
import com.ibm.amoeba.server.store.Bootstrap
import com.ibm.amoeba.server.store.backend.MapBackend
import com.ibm.amoeba.server.store.cache.SimpleLRUObjectCache
import com.ibm.amoeba.server.transaction.{TransactionDriver, TransactionFinalizer}

import scala.concurrent.{ExecutionContext, Future}


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
    override def create(txd: TransactionDescription, messenger: ServerMessenger): TransactionFinalizer = new NullFinalizer
  }


  class TClient(msngr: ClientMessenger) extends AmoebaClient {

    override val clientId: ClientId = ClientId(new UUID(0,1))

    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache

    def backgroundTasks: BackgroundTask = BackgroundTask.NoBackgroundTasks

    def clientContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    def opportunisticRebuildManager: OpportunisticRebuildManager = OpportunisticRebuildManager.None

    val messenger: ClientMessenger = msngr

    val objectCache: ObjectCache = ObjectCache.NoCache

    def receiveClientResponse(msg: ClientResponse): Unit = ()

    def getSystemAttribute(key: String): Option[String] = None
    def setSystemAttribute(key: String, value: String): Unit = ()
  }
}


class TestNetwork extends ServerMessenger {
  import TestNetwork._

  val objectCache = new SimpleLRUObjectCache(1000)

  val poolId = PoolId(new UUID(0, 0))

  val storeId0 = StoreId(poolId, 0)
  val storeId1 = StoreId(poolId, 1)
  val storeId2 = StoreId(poolId, 2)

  val store0 = new MapBackend(storeId0)
  val store1 = new MapBackend(storeId1)
  val store2 = new MapBackend(storeId2)

  val ida = Replication(3, 2)

  val nucleus: KeyValueObjectPointer = Bootstrap.initialize(ida, List(store0, store1, store2))

  val smgr = new StoreManager(objectCache, this, BackgroundTask.NoBackgroundTasks,
    TestCRL, NullFinalizer, TransactionDriver.noErrorRecoveryFactory,
    List(store0, store1, store2))


  private val cliMessenger = new ClientMessenger {

    def sendClientRequest(msg: ClientRequest): Unit = smgr.receiveClientRequest(msg)

    def sendTransactionMessage(msg: TxMessage): Unit = smgr.receiveTransactionMessage(msg)

    def sendTransactionMessages(msg: List[TxMessage]): Unit = msg.foreach(m => smgr.receiveTransactionMessage(m))

  }

  val client = new TClient(cliMessenger)

  // process load store events
  smgr.handleEvents()

  private var topSend = true

  override def sendClientResponse(msg: ClientResponse): Unit = client.receiveClientResponse(msg)

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
