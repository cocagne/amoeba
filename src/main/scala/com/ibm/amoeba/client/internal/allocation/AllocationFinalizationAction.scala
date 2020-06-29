package com.ibm.amoeba.client.internal.allocation

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import com.ibm.amoeba.client.{AmoebaClient, FinalizationAction, FinalizationActionFactory, RegisteredTypeFactory, Transaction}
import com.ibm.amoeba.common.objects.{Key, ObjectPointer, Value}
import com.ibm.amoeba.common.transaction.FinalizationActionId
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}

class AllocationFinalizationAction(val client: AmoebaClient,
                                   val newObject: ObjectPointer) extends FinalizationAction with Logging {

  implicit val ec: ExecutionContext = client.clientContext

  private val completionPromise: Promise[Unit] = Promise()

  def complete: Future[Unit] = completionPromise.future

  def execute(): Unit = {
    println(s"**** AllocationFA for ${newObject.id} EXECUTE")
    val fcomplete = client.retryStrategy.retryUntilSuccessful {
      logger.trace(s"**** AllocationFA for ${newObject.id} Start")
      for {
        pool <- client.getStoragePool(newObject.poolId)
        _=logger.trace(s"**** AllocationFA for ${newObject.id} Got Pool")
        tx = client.newTransaction()
        _ <- pool.allocationTree.set(Key(newObject.toArray), Value(Array()))(tx)
        _=logger.trace(s"**** AllocationFA for ${newObject.id} Tx Prepped")
        _ <- tx.commit()
      } yield {
        completionPromise.success(())
      }
    }
  }
}

object AllocationFinalizationAction extends RegisteredTypeFactory with FinalizationActionFactory {
  val typeUUID: UUID = UUID.fromString("821049AD-D2A8-4D17-8080-E01A4678C8B2")

  def createFinalizationAction(client: AmoebaClient, data: Array[Byte]): FinalizationAction = {
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    val newObject = ObjectPointer.fromByteBuffer(bb)

    println(s"****************** CRATED ALLOC FA for ${newObject.id}")

    new AllocationFinalizationAction(client, newObject)
  }

  def addToTransaction(newObject: ObjectPointer,
                       tx: Transaction): Unit = {
    tx.addFinalizationAction(FinalizationActionId(typeUUID), Some(newObject.toArray))
  }

}