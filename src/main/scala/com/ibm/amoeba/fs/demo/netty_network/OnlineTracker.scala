package com.ibm.amoeba.fs.demo.netty_network

import java.util.UUID

import com.ibm.amoeba.common.objects.ObjectPointer
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.fs.demo.ConfigFile
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future}

object OnlineTracker {
  val rnd = new java.util.Random
}

class OnlineTracker(config: ConfigFile.Config) extends Logging {

  import OnlineTracker._

  class NStorageHost(val cfg: ConfigFile.StorageNode) {

    val uuid: UUID = cfg.uuid

    def name: String = cfg.name

    private var isOnline = false

    def online: Boolean = synchronized { isOnline }

    def setOnline(v: Boolean): Unit = synchronized { isOnline = v }

    def ownsStore(storeId: StoreId)(implicit ec: ExecutionContext): Future[Boolean] = Future.successful(true)
  }

  val nodes: Map[UUID, NStorageHost] = config.nodes.map(n => n._2.uuid -> new NStorageHost(n._2) )

  val storeToHost: Map[StoreId, NStorageHost] = config.nodes.values.foldLeft(Map[StoreId, NStorageHost]()) { (m, n) =>

    n.stores.foldLeft(m) { (sm, s) =>
      sm + (StoreId(PoolId(config.pools(s.pool).uuid), s.store.asInstanceOf[Byte]) -> nodes(n.uuid))
    }
  }

  def setNodeOnline(nodeUUID: UUID): Unit = nodes.get(nodeUUID).foreach { host =>
    synchronized {
      logger.info(s"Node Online: ${host.name}")
      host.setOnline(true)
    }
  }

  def setNodeOffline(nodeUUID: UUID): Unit = nodes.get(nodeUUID).foreach { host =>
    synchronized {
      logger.info(s"Node Offline: ${host.name}")
      host.setOnline(false)
    }
  }

  def isNodeOnline(nodeUUID: UUID): Boolean = nodes(nodeUUID).online

  //def getStorageHostForStore(storeId: StoreId): Future[StorageHost] = Future.successful(storeToHost(storeId))

  def chooseDesignatedLeader(p: ObjectPointer): Byte = {
    var attempts = 0
    var online = false
    var idx = 0.asInstanceOf[Byte]

    while(!online && attempts < p.ida.width * 2) {
      attempts += 1
      idx = rnd.nextInt(p.ida.width).asInstanceOf[Byte]
      online = storeToHost(StoreId(p.poolId, idx)).online
    }

    idx
  }
}