package com.ibm.amoeba.client.internal.network

import com.ibm.amoeba.common.network.{ClientId, ClientRequest, TxMessage}

trait Messenger {

  def sendClientRequest(msg: ClientRequest): Unit

  def sendTransactionMessage(msg: TxMessage): Unit

  def sendTransactionMessages(msg: List[TxMessage]): Unit

}
