package com.ibm.amoeba.fs.demo.network

import com.ibm.amoeba.codec
import com.ibm.amoeba.common.network.Codec
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.fs.demo.StoreConfig
import com.ibm.amoeba.server.StoreManager
import com.ibm.amoeba.server.cnc.*
import com.ibm.amoeba.server.store.backend.{RocksDBBackend, RocksDBType}
import org.apache.logging.log4j.scala.Logging
import org.zeromq.SocketType

import java.nio.file.Path
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.{Future, Promise}

import scala.concurrent.ExecutionContext.Implicits.global

class ZCnCBackend(val network: ZMQNetwork,
                  val storesDir: Path,
                  val storeManagers: List[StoreManager],
                  val cncPort: Int) extends Logging:

  private val completionQueue = new LinkedBlockingQueue[String]()

  private val repSocket = network.context.createSocket(SocketType.REP)
  repSocket.bind(s"tcp://*:$cncPort")

  private val serviceThread = new Thread {
    override def run(): Unit = {
      service()
    }
  }
  serviceThread.start()

  private def service(): Unit =
    while true do

      val encodedMessage = repSocket.recv()

      val bb = ByteBuffer.wrap(encodedMessage)
      bb.order(ByteOrder.BIG_ENDIAN)

      val r = try Some(codec.CnCMessage.parseFrom(bb)) catch
        case t: Throwable =>
          logger.error(s"******* PARSE CNC MESSAGE ERROR: $t", t)
          None

      r match
        case None =>
        case Some(cmsg) =>

          val replyBuilder = codec.CnCReply.newBuilder()

          if cmsg.hasNewStore then
            val message = Codec.decode(cmsg.getNewStore)
            logger.trace(s"Got CnC message $message")
            onNewStore(message)

          if cmsg.hasShutdownStore then
            val message = Codec.decode(cmsg.getShutdownStore)
            logger.trace(s"Got CnC message $message")
            onShutdownStore(message)

          if cmsg.hasTransferStore then
            val message = Codec.decode(cmsg.getTransferStore)
            logger.trace(s"Got CnC message $message")
            onTransferStore(message)

          // Wait for operation completion
          val m = completionQueue.take()
          
          val okBuilder = codec.CnCOk.newBuilder()

          replyBuilder.setOk(okBuilder.build)

          val rmsg = replyBuilder.build.toByteArray

          repSocket.send(rmsg)


  def onNewStore(msg: NewStore): Unit =
    val backend = msg.backendType match
      case b: RocksDBType =>
        val dir = storesDir.resolve(s"${msg.storeId.poolId.uuid}:${msg.storeId.poolIndex}")
        println(s"Creating NEW data store ${msg.storeId.poolId.uuid}:${msg.storeId.poolIndex}. Path $dir")
        new RocksDBBackend(dir, msg.storeId,
          scala.concurrent.ExecutionContext.Implicits.global)

    storeManagers.head.loadStore(backend).foreach(_ => completionQueue.put(""))

  def onShutdownStore(msg: ShutdownStore): Unit =
    storeManagers.find(_.containsStore(msg.storeId)) match
      case None => completionQueue.put("")
      case Some(mgr) => mgr.closeStore(msg.storeId).foreach(_ => completionQueue.put(""))

  def onTransferStore(msg: TransferStore): Future[Unit] = ???
