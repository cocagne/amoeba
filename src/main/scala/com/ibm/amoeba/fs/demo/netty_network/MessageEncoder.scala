package com.ibm.amoeba.fs.demo.netty_network

import java.nio.{ByteBuffer, ByteOrder}

import com.google.flatbuffers.FlatBufferBuilder
import com.ibm.amoeba.client.internal.transaction.TransactionBuilder.TransactionData
import com.ibm.amoeba.common.network.protocol.{Message => PMessage}
import com.ibm.amoeba.common.network._
import com.ibm.amoeba.common.objects.Metadata

object MessageEncoder {

  def encodeMessage(message: TxMessage): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)

    var updateContent: Option[TransactionData] = None

    val encodedMsg = message match {
      case m: TxPrepare =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addPrepare(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        updateContent = Some(TransactionData(m.objectUpdates, m.preTxRebuilds))

        builder.sizedByteArray()

      case m: TxPrepareResponse =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addPrepareResponse(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TxAccept =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addAccept(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TxAcceptResponse =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addAcceptResponse(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TxResolved =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addResolved(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TxCommitted =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addCommitted(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TxFinalized =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addFinalized(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TxHeartbeat =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addHeartbeat(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TxStatusRequest =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addHeartbeat(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TxStatusResponse =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addHeartbeat(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()
    }

    val (contentSize, preTxSize) = updateContent match {
      case None => (0, 0)
      case Some(td) =>
        val luSize = td.localUpdates.foldLeft(0)((sz, lu) => sz + 16 + 4 + lu.data.size)
        val lpSize = td.preTransactionRebuilds.foldLeft(0)((sz, p) => sz + 16 + Metadata.EncodedSize + 4 + p.data.size)

        (luSize, lpSize)
    }

    val msg = new Array[Byte](4 + encodedMsg.length + 4 + 4 + contentSize + preTxSize)
    val bb = ByteBuffer.wrap(msg)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)
    bb.putInt(contentSize)
    bb.putInt(preTxSize)

    updateContent.foreach { td =>

      td.localUpdates.foreach { lu =>
        bb.putLong(lu.objectId.uuid.getMostSignificantBits)
        bb.putLong(lu.objectId.uuid.getLeastSignificantBits)
        bb.putInt(lu.data.size)
        bb.put(lu.data.asReadOnlyBuffer())
      }

      td.preTransactionRebuilds.foreach { pt =>
        bb.putLong(pt.objectId.uuid.getMostSignificantBits)
        bb.putLong(pt.objectId.uuid.getLeastSignificantBits)
        pt.requiredMetadata.encodeInto(bb)
        bb.putInt(pt.data.size)
        bb.put(pt.data.asReadOnlyBuffer())
      }
    }

    msg
  }

  def encodeMessage(message: ClientRequest): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)

    val encodedMsg = message match {
      case m: Read =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addRead(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: Allocate =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addAllocate(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: OpportunisticRebuild =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addOpportunisticRebuild(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TransactionCompletionQuery =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addTransactionCompletionQuery(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()
    }

    val msg = new Array[Byte](4 + encodedMsg.length)
    val bb = ByteBuffer.wrap(msg)

    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)

    msg
  }

  def encodeMessage(message: ClientResponse): Array[Byte] = {
    val builder = new FlatBufferBuilder(4096)

    val encodedMsg = message match {

      case m: ReadResponse =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addReadResponse(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: AllocateResponse =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addAllocateResponse(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TransactionCompletionResponse =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addTransactionCompletionResponse(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TransactionFinalized =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addTxFinalized(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()

      case m: TransactionResolved =>
        val o = NetworkCodec.encode(builder, m)

        PMessage.startMessage(builder)
        PMessage.addTxResolved(builder, o)

        PMessage.finishMessageBuffer(builder, PMessage.endMessage(builder))

        builder.sizedByteArray()
    }

    val msg = new Array[Byte](4 + encodedMsg.length)
    val bb = ByteBuffer.wrap(msg)

    bb.putInt(encodedMsg.length)
    bb.put(encodedMsg)

    msg
  }
}
