package com.ibm.amoeba.client

import com.ibm.amoeba.client.internal.OpportunisticRebuildManager
import com.ibm.amoeba.client.internal.network.Messenger
import com.ibm.amoeba.common.network.ClientId

import scala.concurrent.ExecutionContext

trait AmoebaClient {

  val clientId: ClientId

  val txStatusCache: TransactionStatusCache

  private[client] def clientContext: ExecutionContext

  private[client] def opportunisticRebuildManager: OpportunisticRebuildManager

  private[client] val messenger: Messenger

  private[client] val objectCache: ObjectCache

  private[amoeba] def getSystemAttribute(key: String): Option[String]
  private[amoeba] def setSystemAttribute(key: String, value: String): Unit
}
