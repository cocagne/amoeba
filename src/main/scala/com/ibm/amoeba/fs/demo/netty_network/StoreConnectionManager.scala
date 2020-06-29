package com.ibm.amoeba.fs.demo.netty_network

import com.ibm.amoeba.common.network.ClientId
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelOption}
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.logging.log4j.scala.Logging

class StoreConnectionManager(val storeNetwork: StoreNetwork,
                             val port: Int) extends Logging {

  private val serverBoot = new ServerBootstrap

  private[this] var clientConnections = Map[ClientId, ChannelHandlerContext]()

  class StoreChannelHandler(snet: StoreNetwork) extends ChannelInboundHandlerAdapter {

    // UUID of the client. Set in first message from client
    var oclientId: Option[ClientId] = None

    override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = synchronized {
      msg match {
        case arr: Array[Byte] => oclientId match {
          case None =>
            if (arr.length == 16) {
              val clientId = ClientId(com.ibm.amoeba.common.util.byte2uuid(arr))
              oclientId = Some(clientId)
              updateClientConnetion(clientId, Some(ctx))
            } else
              logger.error(s"RECEIVED UNEXPECTED INITIAL MESSAGE OF SIZE: ${arr.length}")

          case Some(_) => storeNetwork.receiveMessage(arr)
        }

        case x => logger.error(s"RECEIVED UNEXPECTED MESSAGE TYPE: $x")
      }
    }

    override def channelInactive(ctx: ChannelHandlerContext): Unit = synchronized {
      oclientId.foreach { clientUUID =>
        updateClientConnetion(clientUUID, None)
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      // Close the connection when an exception is raised.
      cause.printStackTrace()
      ctx.close()
    }
  }

  def updateClientConnetion(clientId: ClientId, octx: Option[ChannelHandlerContext]): Unit = synchronized {
    octx match {
      case None => clientConnections -= clientId
      case Some(ctx) => clientConnections += (clientId -> ctx)
    }
  }

  def sendMessageToClient(clientId: ClientId, msg: Array[Byte]): Unit = synchronized {
    clientConnections.get(clientId).foreach(_.writeAndFlush(msg))
  }

  serverBoot.group(storeNetwork.nnet.serverBossGroup, storeNetwork.nnet.serverWorkerGroup)
    .channel(classOf[NioServerSocketChannel])
    .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 100)
    .handler(new LoggingHandler(LogLevel.INFO))
    .childHandler(new AmoebaChannelInitializer {
      def newChannel() = new StoreChannelHandler(storeNetwork)
    })

  // Start the server.
  serverBoot.bind(port).sync()
}
