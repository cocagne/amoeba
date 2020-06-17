package com.ibm.amoeba.fs.demo.network

import java.util.UUID

import com.ibm.amoeba.client.AmoebaClient
import com.ibm.amoeba.common.network.ClientId
import com.ibm.amoeba.fs.demo.ConfigFile
import io.netty.channel.nio.NioEventLoopGroup

class NettyNetwork(val config: ConfigFile.Config, oclientId: Option[ClientId]) {

  val clientId: ClientId = oclientId.getOrElse(ClientId(UUID.randomUUID()))

  val serverBossGroup = new NioEventLoopGroup(1)
  val serverWorkerGroup = new NioEventLoopGroup
  val clientWorkerGroup = new NioEventLoopGroup

  private var stores: List[StoreNetwork] = Nil
  private var oclient: Option[ClientNetwork] = None

  def setClient(client: AmoebaClient): Unit = {
    stores.foreach(_.setClient(client))
    oclient.foreach(_.setClient(client))
  }

  def createStoreNetwork(nodeName: String): StoreNetwork = synchronized {
    val s = new StoreNetwork(nodeName, this)
    stores = s :: stores
    s
  }

  def createClientNetwork(): ClientNetwork = synchronized {
    val c = new ClientNetwork(this, clientId)
    oclient = Some(c)
    c
  }

  def shutdown(): Unit = {
    serverBossGroup.shutdownGracefully()
    serverWorkerGroup.shutdownGracefully()
    clientWorkerGroup.shutdownGracefully()
  }

}
