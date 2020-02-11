package com.ibm.amoeba.client.tkvl

import com.ibm.amoeba.client.AmoebaClient

trait RootManagerFactory {
  def createRootManager(client: AmoebaClient, data: Array[Byte]): RootManager
}
