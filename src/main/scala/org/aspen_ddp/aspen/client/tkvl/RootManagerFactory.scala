package org.aspen_ddp.aspen.client.tkvl

import org.aspen_ddp.aspen.client.AmoebaClient

trait RootManagerFactory {
  def createRootManager(client: AmoebaClient, data: Array[Byte]): RootManager
}
