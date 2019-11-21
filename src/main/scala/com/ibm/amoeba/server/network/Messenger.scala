package com.ibm.amoeba.server.network

import com.ibm.amoeba.common.network.{ClientResponse, TxMessage}

trait Messenger {

  def sendClientResponse(msg: ClientResponse): Unit

  def sendTransactionMessage(msg: TxMessage): Unit

}
