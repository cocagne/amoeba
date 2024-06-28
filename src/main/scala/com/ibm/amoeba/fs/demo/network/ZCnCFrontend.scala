package com.ibm.amoeba.fs.demo.network

import com.ibm.amoeba.codec
import com.ibm.amoeba.common.network.Codec
import com.ibm.amoeba.server.cnc.{CnCFrontend, CnCMessage, NewStore, ShutdownStore, TransferStore}
import org.apache.logging.log4j.scala.Logging
import org.zeromq.SocketType

import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.{Future, Promise}

object ZCnCFrontend:
  sealed abstract class QMsg

  final case class CnC(msg: CnCMessage, promise: Promise[Unit]) extends QMsg

  final case class Shutdown(promise: Promise[Unit]) extends QMsg

class ZCnCFrontend(val network: ZMQNetwork,
                   val hostAddr: String,
                   val cncPort: Int) extends CnCFrontend with Logging:

  import ZCnCFrontend._

  private val msgQueue = new LinkedBlockingQueue[QMsg]()

  private val reqSocket = network.context.createSocket(SocketType.REQ)
  reqSocket.connect(s"tcp://$hostAddr:$cncPort")

  private val networkThread = new Thread {
    override def run(): Unit = {
      ioThread()
    }
  }
  networkThread.start()

  private def ioThread(): Unit =
    while true do
      val m = msgQueue.take()
      m match
        case Shutdown(p) =>
          p.success(())
          return
        case CnC(msg, p) =>
          val encodedMessage = msg match
            case msg: NewStore => Codec.encode(msg).toByteArray
            case msg: ShutdownStore => Codec.encode(msg).toByteArray
            case msg: TransferStore => Codec.encode(msg).toByteArray

          reqSocket.send(encodedMessage)

          // TODO: Parse result and use explicit return messages.
          //       For now just return success
          reqSocket.recv()
          
          p.success(())
  
  def send(msg: NewStore): Future[Unit] =
    val p = Promise[Unit]
    msgQueue.put(CnC(msg, p))
    p.future

  def send(msg: ShutdownStore): Future[Unit] =
    val p = Promise[Unit]
    msgQueue.put(CnC(msg, p))
    p.future

  def send(msg: TransferStore): Future[Unit] =
    val p = Promise[Unit]
    msgQueue.put(CnC(msg, p))
    p.future
