package com.ibm.amoeba.client.internal.allocation
import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import com.ibm.amoeba.client.{AmoebaClient, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, Transaction}
import com.ibm.amoeba.common.objects.{Key, ObjectPointer, Value}
import com.ibm.amoeba.common.transaction.{FinalizationActionId, SerializedFinalizationAction, TransactionDescription}
import org.apache.logging.log4j.scala.{Logger, Logging}

import scala.concurrent.{ExecutionContext, Future, Promise}

class DeletionFinalizationAction(val client: AmoebaClient,
                                 val txd: TransactionDescription,
                                 val deletedObject: ObjectPointer) extends FinalizationAction {

  implicit val ec: ExecutionContext = client.clientContext

  private val completionPromise: Promise[Unit] = Promise()
  
  logger.debug(s"Created DeletionFinalizationAction for object ${deletedObject.id}")

  def complete: Future[Unit] = completionPromise.future

  def execute(): Unit = {
    val fcomplete = client.retryStrategy.retryUntilSuccessful {
      for {
        pool <- client.getStoragePool(deletedObject.poolId)
        tx = client.newTransaction()
        _ <- pool.allocationTree.delete(Key(deletedObject.toArray))(tx)
        _ <- tx.commit()
      } yield {
        completionPromise.success(())
      }
    }
  }
}

object DeletionFinalizationAction extends RegisteredTypeFactory with FinalizationActionFactory {
  val typeUUID: UUID = UUID.fromString("35A0AA38-F066-406F-9778-287F7C49012C")

  def createFinalizationAction(client: AmoebaClient,
                               txd: TransactionDescription,
                               data: Array[Byte]): FinalizationAction = {
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    val deletedObject = ObjectPointer.fromByteBuffer(bb)

    new DeletionFinalizationAction(client, txd, deletedObject)
  }

  def createSerializedFA(deletedObject: ObjectPointer): SerializedFinalizationAction = {
    SerializedFinalizationAction(FinalizationActionId(typeUUID), deletedObject.toArray)
  }

  def addToTransaction(deletedObject: ObjectPointer,
                       tx: Transaction): Unit = {
    tx.addFinalizationAction(FinalizationActionId(typeUUID), Some(deletedObject.toArray))
  }

}

