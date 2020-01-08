package com.ibm.amoeba.client

import com.ibm.amoeba.client.internal.OpportunisticRebuildManager
import com.ibm.amoeba.client.internal.network.Messenger
import com.ibm.amoeba.common.network.{ClientId, ClientResponse, TxMessage}
import com.ibm.amoeba.common.objects.{DataObjectPointer, KeyValueObjectPointer}
import com.ibm.amoeba.common.util.BackgroundTask

import scala.concurrent.{ExecutionContext, Future}

trait AmoebaClient {

  val clientId: ClientId

  val txStatusCache: TransactionStatusCache

  def read(pointer: DataObjectPointer): Future[DataObjectState]

  def read(pointer: KeyValueObjectPointer): Future[KeyValueObjectState]

  private[client] def backgroundTasks: BackgroundTask

  private[client] def clientContext: ExecutionContext

  private[client] def opportunisticRebuildManager: OpportunisticRebuildManager

  private[client] val messenger: Messenger

  private[client] val objectCache: ObjectCache

  private[amoeba] def receiveClientResponse(msg: ClientResponse): Unit

  private[amoeba] def getSystemAttribute(key: String): Option[String]
  private[amoeba] def setSystemAttribute(key: String, value: String): Unit
}
