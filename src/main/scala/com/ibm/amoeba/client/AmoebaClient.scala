package com.ibm.amoeba.client

import com.ibm.amoeba.client.internal.OpportunisticRebuildManager
import com.ibm.amoeba.client.internal.allocation.AllocationManager
import com.ibm.amoeba.client.internal.network.Messenger
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.network.{ClientId, ClientResponse}
import com.ibm.amoeba.common.objects.{DataObjectPointer, KeyValueObjectPointer}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.TransactionDescription
import com.ibm.amoeba.common.util.BackgroundTask
import com.ibm.amoeba.server.cnc.{CnCFrontend, NewStore}
import com.ibm.amoeba.server.store.backend.BackendType

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait AmoebaClient extends ObjectReader {

  val clientId: ClientId

  val txStatusCache: TransactionStatusCache

  val typeRegistry: TypeRegistry

  def client: AmoebaClient = this

  def shutdown(): Unit = ()

  def read(pointer: DataObjectPointer, comment: String): Future[DataObjectState]

  def read(pointer: KeyValueObjectPointer, comment: String): Future[KeyValueObjectState]

  def newTransaction(): Transaction

  def getStoragePool(poolId: PoolId): Future[Option[StoragePool]]

  def getStoragePool(poolName: String): Future[Option[StoragePool]]
  
  def updateStorageHost(storeId: StoreId, newHostId: HostId): Future[Unit]
  
  def newStoragePool(newPoolName: String, 
                     hostCncFrontends: List[CnCFrontend], 
                     ida: IDA,
                     backendType: BackendType): Future[StoragePool] =

    implicit val ec: ExecutionContext = this.clientContext

    val newPoolId = PoolId(UUID.randomUUID())
    
    for 
      _ <- Future.sequence(hostCncFrontends.zipWithIndex.map { (fend, idx) =>
        fend.send(NewStore(StoreId(newPoolId, idx.toByte), backendType))
      })
      poolCfg = StoragePool.Config(
        newPoolId, newPoolName, ida.width, ida, None, hostCncFrontends.map(_.host.hostId).toArray
      )
      newStoragePool <- createStoragePool(poolCfg)
    yield
      newStoragePool
      
  protected def createStoragePool(config: StoragePool.Config): Future[StoragePool]

  def getHost(hostId: HostId): Future[Option[Host]]

  def getHost(hostName: String): Future[Option[Host]]

  def transact[T](prepare: Transaction => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val tx = newTransaction()

    val fprep = try { prepare(tx) } catch {
      case err: Throwable => Future.failed(err)
    }

    val fresult = for {
      prepResult <- fprep
      _ <- tx.commit()
    } yield prepResult

    fresult.failed.foreach(err => tx.invalidateTransaction(err))

    fresult
  }

  def transactUntilSuccessful[T](prepare: Transaction => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    retryStrategy.retryUntilSuccessful {
      transact(prepare)
    }
  }
  def transactUntilSuccessfulWithRecovery[T](onCommitFailure: Throwable => Future[Unit])(prepare: Transaction => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    retryStrategy.retryUntilSuccessful(onCommitFailure) {
      transact(prepare)
    }
  }

  def retryStrategy: RetryStrategy

  def backgroundTasks: BackgroundTask

  def clientContext: ExecutionContext

  private[client] def opportunisticRebuildManager: OpportunisticRebuildManager

  private[client] val messenger: Messenger

  private[client] val allocationManager: AllocationManager

  private[client] val objectCache: ObjectCache

  private[amoeba] def receiveClientResponse(msg: ClientResponse): Unit

  private[amoeba] def getSystemAttribute(key: String): Option[String]
  private[amoeba] def setSystemAttribute(key: String, value: String): Unit

}
