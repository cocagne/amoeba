package com.ibm.amoeba.client.internal.network

import com.ibm.amoeba.common.network.{ClientRequest, TxMessage}

trait Messenger {

  def sendClientRequest(msg: ClientRequest): Unit

  def sendTransactionMessage(msg: TxMessage): Unit

  def sendTransactionMessages(msg: List[TxMessage]): Unit

}

object Messenger {
  object None extends Messenger {
    def sendClientRequest(msg: ClientRequest): Unit = ()

    def sendTransactionMessage(msg: TxMessage): Unit = ()

    def sendTransactionMessages(msg: List[TxMessage]): Unit = ()
  }
}
