package com.ibm.amoeba.server.network

import com.ibm.amoeba.common.network.{ClientId, ClientMessage}
import com.ibm.amoeba.common.objects.ObjectId
import com.ibm.amoeba.common.store.{ReadError, ReadState}

trait Messenger {
  def sendReadResponse(clientId: ClientId,
                       requestId: RequestId,
                       objectId: ObjectId,
                       result: Either[ReadState, ReadError.Value]): Unit

  def sendClientMessage(msg: ClientMessage): Unit
}
