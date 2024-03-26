package com.ibm.amoeba.client.internal.allocation

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import com.ibm.amoeba.client.{AmoebaClient, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, Transaction}
import com.ibm.amoeba.common.objects.{Key, ObjectPointer, Value}
import com.ibm.amoeba.common.transaction.{FinalizationActionId, TransactionDescription}
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
    logger.trace(s"**** AllocationFA for Tx ${txd.transactionId}, Object ${newObject.id} Start")
    for {
      pool <- client.getStoragePool(newObject.poolId)
      _=logger.trace(s"**** AllocationFA for Tx ${txd.transactionId}, Object ${newObject.id} Got Pool")
      tx = client.newTransaction()
      _ <- pool.allocationTree.set(Key(newObject.toArray), Value(Array()))(tx)
      _=logger.trace(s"**** AllocationFA for Tx ${txd.transactionId}, Object ${newObject.id} Tx Prepped. Txid: ${tx.id}")
      _ <- tx.commit()
    } yield {
      logger.trace(s"**** AllocationFA Completed Successfully for Tx ${txd.transactionId}, Object ${newObject.id}")
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