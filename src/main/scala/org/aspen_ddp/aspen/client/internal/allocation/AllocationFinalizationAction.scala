package org.aspen_ddp.aspen.client.internal.allocation

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import org.aspen_ddp.aspen.client.{AmoebaClient, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, Transaction}
import org.aspen_ddp.aspen.common.objects.{Key, ObjectPointer, Value}
import org.aspen_ddp.aspen.common.transaction.{FinalizationActionId, TransactionDescription}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}

class AllocationFinalizationAction(val client: AmoebaClient,
                                   val txd: TransactionDescription,
                                   val newObject: ObjectPointer) extends FinalizationAction with Logging {

  implicit val ec: ExecutionContext = client.clientContext

  private val completionPromise: Promise[Unit] = Promise()

  logger.debug(s"Created AllocationFinalizationAction for Tx ${txd.transactionId}, object ${newObject.id}")
  def complete: Future[Unit] = completionPromise.future

  def execute(): Unit = client.retryStrategy.retryUntilSuccessful {
    for {
      pool <- client.getStoragePool(newObject.poolId)
      tx = client.newTransaction()
      _ <- pool.get.allocationTree.set(Key(newObject.id.toBytes), Value(newObject.toArray))(tx)
      _ <- tx.commit()
    } yield {
      logger.debug(s"AllocationFA Completed Successfully for Tx ${txd.transactionId}, Object ${newObject.id}")
      completionPromise.success(())
    }
  }
}

object AllocationFinalizationAction extends RegisteredTypeFactory with FinalizationActionFactory {
  val typeUUID: UUID = UUID.fromString("821049AD-D2A8-4D17-8080-E01A4678C8B2")

  def createFinalizationAction(client: AmoebaClient,
                               txd: TransactionDescription,
                               data: Array[Byte]): FinalizationAction = {
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    val newObject = ObjectPointer.fromByteBuffer(bb)

    //println(s"****************** CRATED ALLOC FA for ${newObject.id}")

    new AllocationFinalizationAction(client, txd, newObject)
  }

  def addToTransaction(newObject: ObjectPointer,
                       tx: Transaction): Unit = {
    tx.addFinalizationAction(FinalizationActionId(typeUUID), Some(newObject.toArray))
  }

}