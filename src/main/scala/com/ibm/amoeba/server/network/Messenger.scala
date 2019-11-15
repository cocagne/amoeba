package com.ibm.amoeba.server.network

import com.ibm.amoeba.common.network.ClientResponse

trait Messenger {

  def sendClientResponse(msg: ClientResponse): Unit

}
