package com.ibm.amoeba

import java.util.UUID

import com.ibm.amoeba
import com.ibm.amoeba.client.internal.{OpportunisticRebuildManager, StaticTypeRegistry}
import com.ibm.amoeba.client.internal.allocation.{AllocationManager, BaseAllocationDriver}
import com.ibm.amoeba.client.{AmoebaClient, DataObjectState, ExponentialBackoffRetryStrategy, KeyValueObjectState, ObjectCache, RetryStrategy, StoragePool, Transaction, TransactionStatusCache, TypeRegistry}
import com.ibm.amoeba.client.internal.network.{Messenger => ClientMessenger}
import com.ibm.amoeba.client.internal.pool.SimpleStoragePool
import com.ibm.amoeba.client.internal.read.{BaseReadDriver, ReadManager}
import com.ibm.amoeba.client.internal.transaction.{ClientTransactionDriver, TransactionImpl, TransactionManager}
import com.ibm.amoeba.common.Nucleus
import com.ibm.amoeba.common.ida.Replication
import com.ibm.amoeba.common.network.{AllocateResponse, ClientId, ClientRequest, ClientResponse, ReadResponse, TransactionCompletionResponse, TransactionFinalized, TransactionResolved, TxMessage}
import com.ibm.amoeba.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{TransactionDescription, TransactionId}
import com.ibm.amoeba.common.util.{BackgroundTask, BackgroundTaskPool}
import com.ibm.amoeba.server.{StoreManager, transaction}
import com.ibm.amoeba.server.crl.{AllocSaveComplete, AllocationRecoveryState, CrashRecoveryLog, CrashRecoveryLogClient, CrashRecoveryLogFactory, SaveCompletionHandler, TransactionRecoveryState, TxSaveComplete, TxSaveId}
import com.ibm.amoeba.server.network.{Messenger => ServerMessenger}
import com.ibm.amoeba.server.store.Bootstrap
import com.ibm.amoeba.server.store.backend.MapBackend
import com.ibm.amoeba.server.store.cache.SimpleLRUObjectCache
import com.ibm.amoeba.server.transaction.{TransactionDriver, TransactionFinalizer}

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}


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

  /*
  class NullFinalizer extends TransactionFinalizer {
    override def complete: Future[Unit] = Future.successful(())

    override def updateCommitErrors(commitErrors: Map[StoreId, List[ObjectId]]): Unit = ()


    override def debugStatus: List[(String, Boolean)] = Nil

    override def cancel(): Unit = ()
  }

  object NullFinalizer extends TransactionFinalizer.Factory {
    override def create(txd: TransactionDescription, messenger: ServerMessenger): TransactionFinalizer = new NullFinalizer
  }
  */

  class TClient(msngr: ClientMessenger) extends AmoebaClient {

    import scala.concurrent.ExecutionContext.Implicits.global

    var attributes: Map[String, String] = Map()

    override val clientId: ClientId = ClientId(new UUID(0,1))

    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache

    val typeRegistry: TypeRegistry = new TypeRegistry(StaticTypeRegistry.types.toMap)

    val rmgr = new ReadManager(this, BaseReadDriver.noErrorRecoveryReadDriver)

    def read(pointer: DataObjectPointer): Future[DataObjectState] = {
      rmgr.read(pointer).map(_.asInstanceOf[DataObjectState])
    }

    def read(pointer: KeyValueObjectPointer): Future[KeyValueObjectState] = {
      rmgr.read(pointer).map(_.asInstanceOf[KeyValueObjectState])
    }

    val txManager = new TransactionManager(this, ClientTransactionDriver.noErrorRecoveryFactory)

    def newTransaction(): Transaction = {
      new TransactionImpl(this, txManager, _ => 0, None)
    }

    def getStoragePool(poolId: PoolId): Future[StoragePool] = Future.successful(new SimpleStoragePool(this,
      poolId, 3, Replication(3,2), None, None))

    val retryStrategy: RetryStrategy = new ExponentialBackoffRetryStrategy(this)

    def backgroundTasks: BackgroundTask = BackgroundTask.NoBackgroundTasks

    def clientContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    def opportunisticRebuildManager: OpportunisticRebuildManager = OpportunisticRebuildManager.None

    val messenger: ClientMessenger = msngr

    val allocationManager: AllocationManager = new AllocationManager(this,
      BaseAllocationDriver.NoErrorRecoveryAllocationDriver)

    val objectCache: ObjectCache = ObjectCache.NoCache

    def receiveClientResponse(msg: ClientResponse): Unit = msg match {
      case m: ReadResponse => rmgr.receive(m)
      case m: TransactionCompletionResponse => rmgr.receive(m)
      case m: TransactionResolved => txManager.receive(m)
      case m: TransactionFinalized => txManager.receive(m)
      case m: AllocateResponse => allocationManager.receive(m)
      case _ =>
    }

    def getSystemAttribute(key: String): Option[String] = attributes.get(key)
    def setSystemAttribute(key: String, value: String): Unit = attributes += key -> value
  }
}


class TestNetwork extends ServerMessenger {
  import TestNetwork._

  val objectCacheFactory: () => SimpleLRUObjectCache = () => new SimpleLRUObjectCache(1000)

  val storeId0 = StoreId(Nucleus.poolId, 0)
  val storeId1 = StoreId(Nucleus.poolId, 1)
  val storeId2 = StoreId(Nucleus.poolId, 2)

  val store0 = new MapBackend(storeId0)
  val store1 = new MapBackend(storeId1)
  val store2 = new MapBackend(storeId2)

  val ida = Replication(3, 2)

  val nucleus: KeyValueObjectPointer = Bootstrap.initialize(ida, List(store0, store1, store2))

  object FinalizerFactory extends TransactionFinalizer.Factory {
    var client: AmoebaClient = null

    def create(txd: TransactionDescription, messenger: ServerMessenger): TransactionFinalizer = {
      new amoeba.server.transaction.TransactionFinalizer.TransactionFinalizerWrapper(client.createFinalizerFor(txd))
    }
  }

  val smgr = new StoreManager(objectCacheFactory, this, BackgroundTask.NoBackgroundTasks,
    TestCRL, FinalizerFactory, TransactionDriver.noErrorRecoveryFactory,
    List(store0, store1, store2))

  private def handleEvents(): Unit = {
    smgr.handleEvents()
    //while (smgr.hasTransactions) {
      //smgr.handleEvents()
    //}
  }

  private val cliMessenger = new ClientMessenger {

    def sendClientRequest(msg: ClientRequest): Unit = {
      smgr.receiveClientRequest(msg)
      handleEvents()
    }

    def sendTransactionMessage(msg: TxMessage): Unit = {
      smgr.receiveTransactionMessage(msg)
      handleEvents()
    }

    def sendTransactionMessages(msg: List[TxMessage]): Unit = sendTransactionMessages(msg)
  }

  val client = new TClient(cliMessenger)

  FinalizerFactory.client = client

  // process load store events
  smgr.handleEvents()

  override def sendClientResponse(msg: ClientResponse): Unit = {
    client.receiveClientResponse(msg)
    handleEvents()
  }

  override def sendTransactionMessage(msg: TxMessage): Unit = {
    smgr.receiveTransactionMessage(msg)
    handleEvents()
  }

  override def sendTransactionMessages(msg: List[TxMessage]): Unit = {
    msg.foreach(smgr.receiveTransactionMessage)
    handleEvents()
  }//msg.foreach(sendTransactionMessage)

  def printTransactionStatus(): Unit = {
    println("*********** Transaction Status ***********")
    smgr.logTransactionStatus(s => println(s))
    println("******************************************")
  }

  def waitForTransactionsToComplete(): Future[Unit] = {
    //val stack = com.ibm.aspen.util.getStack()

    val bgTasks = new BackgroundTaskPool

    val p = Promise[Unit]()
    val pollDelay = Duration(5, MILLISECONDS)

    def check(): Unit = {
      if (!smgr.hasTransactions) {
        bgTasks.shutdown(pollDelay)
        p.success(())
      } else
        bgTasks.schedule(pollDelay)(check())
    }

    bgTasks.schedule(pollDelay)(check())

    p.future
  }
}
