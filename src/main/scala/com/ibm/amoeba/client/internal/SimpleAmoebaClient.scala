package com.ibm.amoeba.client.internal

import com.ibm.amoeba.client.{AmoebaClient, DataObjectState, ExponentialBackoffRetryStrategy, Host, HostId, KeyValueObjectState, ObjectCache, RetryStrategy, StoragePool, Transaction, TransactionStatusCache, TypeRegistry}
import com.ibm.amoeba.client.internal.allocation.{AllocationManager, SuperSimpleAllocationDriver}
import com.ibm.amoeba.common.objects.{DataObjectPointer, Key, KeyValueObjectPointer}
import com.ibm.amoeba.client.internal.network.Messenger as ClientMessenger
import com.ibm.amoeba.client.internal.pool.SimpleStoragePool
import com.ibm.amoeba.client.internal.read.{ReadManager, SimpleReadDriver}
import com.ibm.amoeba.client.internal.transaction.{SimpleClientTransactionDriver, TransactionImpl, TransactionManager}
import com.ibm.amoeba.client.tkvl.{KVObjectRootManager, TieredKeyValueList}
import com.ibm.amoeba.common.Nucleus
import com.ibm.amoeba.common.network.{AllocateResponse, ClientId, ClientResponse, ReadResponse, TransactionCompletionResponse, TransactionFinalized, TransactionResolved}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.util.{BackgroundTask, BackgroundTaskPool}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}

class SimpleAmoebaClient(val msngr: ClientMessenger,
                         override val clientId: ClientId,
                         implicit val executionContext: ExecutionContext,
                         val nucleus: KeyValueObjectPointer,
                         txStatusCacheDuration: FiniteDuration,
                         initialReadDelay: Duration,
                         maxReadDelay: Duration,
                         txRetransmitDelay: Duration,
                         allocationRetransmitDelay: Duration,
                         bootstrapHosts: Map[HostId, Host]) extends AmoebaClient {

  var attributes: Map[String, String] = Map()

  val typeRegistry: TypeRegistry = new TypeRegistry(StaticTypeRegistry.types.toMap)

  override val txStatusCache: TransactionStatusCache = new TransactionStatusCache(txStatusCacheDuration)

  private val rmgr = new ReadManager(this,
    new SimpleReadDriver.Factory(initialReadDelay, maxReadDelay).apply)

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState] = {
    rmgr.read(pointer, comment).map(_.asInstanceOf[DataObjectState])
  }

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState] = {
    rmgr.read(pointer, comment).map(_.asInstanceOf[KeyValueObjectState])
  }

  private val txManager = new TransactionManager(this, SimpleClientTransactionDriver.factory(txRetransmitDelay))

  def newTransaction(): Transaction = {
    new TransactionImpl(this, txManager, _ => 0, None)
  }

  def getStoragePool(poolId: PoolId): Future[StoragePool] = {
    val root = new KVObjectRootManager(this, Nucleus.PoolTreeKey, nucleus)
    val tkvl = new TieredKeyValueList(this, root)
    for {
      poolPtr <- tkvl.get(Key(poolId.uuid))
      poolKvos <- read(KeyValueObjectPointer(poolPtr.get.value.bytes))
    } yield {
      SimpleStoragePool(this, poolKvos)
    }
  }

  def getHost(hostId: HostId): Future[Host] =
    bootstrapHosts.get(hostId) match
      case Some(host) => return Future.successful(host)
      case None =>

    val root = new KVObjectRootManager(this, Nucleus.HostTreeKey, nucleus)
    val tkvl = new TieredKeyValueList(this, root)
    for
      hostValue <- tkvl.get(Key(hostId.uuid))
    yield
      Host(hostValue.get.value.bytes)

  override def shutdown(): Unit = backgroundTasks.shutdown(Duration(50, MILLISECONDS))

  val retryStrategy: RetryStrategy = new ExponentialBackoffRetryStrategy(this)

  def backgroundTasks: BackgroundTask = new BackgroundTaskPool

  def clientContext: ExecutionContext = executionContext

  def opportunisticRebuildManager: OpportunisticRebuildManager = new SimpleOpportunisticRebuildManager(this)

  val messenger: ClientMessenger = msngr

  val allocationManager: AllocationManager = new AllocationManager(this,
    SuperSimpleAllocationDriver.factory(allocationRetransmitDelay))

  val objectCache: ObjectCache = new SimpleObjectCache

  def receiveClientResponse(msg: ClientResponse): Unit = msg match {
    case m: ReadResponse => rmgr.receive(m)
    case m: TransactionCompletionResponse => rmgr.receive(m)
    case m: TransactionResolved => txManager.receive(m)
    case m: TransactionFinalized => txManager.receive(m)
    case m: AllocateResponse => allocationManager.receive(m)
  }

  def getSystemAttribute(key: String): Option[String] = attributes.get(key)
  def setSystemAttribute(key: String, value: String): Unit = attributes += key -> value
}
