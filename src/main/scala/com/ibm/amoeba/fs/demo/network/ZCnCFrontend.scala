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
          val builder = codec.CnCRequest.newBuilder()

          msg match
            case msg: NewStore => builder.setNewStore(Codec.encode(msg))
            case msg: ShutdownStore => builder.setShutdownStore(Codec.encode(msg))
            case msg: TransferStore => builder.setTransferStore(Codec.encode(msg))

          val encodedMessage = builder.build.toByteArray

          reqSocket.send(encodedMessage)

          val rmsg = reqSocket.recv()

          if rmsg != null then
            val bb = ByteBuffer.wrap(rmsg)
            bb.order(ByteOrder.BIG_ENDIAN)
            val m = try codec.CnCReply.parseFrom(bb) catch
              case t: Throwable =>
                logger.error(s"******* PARSE CnCReply ERROR: $t", t)
                p.failure(t)
                throw t

            if m.hasOk then
              p.success(())
            else
              p.failure(new Exception("Invalid CnCReply received"))
  
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
