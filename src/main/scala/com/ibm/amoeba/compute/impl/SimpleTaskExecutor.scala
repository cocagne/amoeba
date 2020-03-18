package com.ibm.amoeba.compute.impl

import com.ibm.amoeba.client.tkvl.{KVObjectRootManager, NodeAllocator}
import com.ibm.amoeba.client.{AmoebaClient, KeyValueObjectState, ObjectAllocator, Transaction}
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, ByteArrayKeyOrdering, Key, KeyValueObjectPointer}
import com.ibm.amoeba.compute.{DurableTaskType, TaskExecutor}

import scala.concurrent.Future

object SimpleTaskExecutor {

  val TreeKey = new Key(Array(0))
  val TreeOrdering = ByteArrayKeyOrdering

  def apply(client: AmoebaClient,
            executorObject: KeyValueObjectPointer,
            kvos: KeyValueObjectState): SimpleTaskExecutor = {
    val rmgr = KVObjectRootManager.createRootManager(client, kvos.contents(TreeKey).value.bytes)

    new SimpleTaskExecutor(client, executorObject, rmgr)
  }

  def createNewExecutor(client: AmoebaClient,
                        executorAllocator: ObjectAllocator,
                        revisionGuard: AllocationRevisionGuard,
                        nodeAllocator: NodeAllocator)
                       (implicit t: Transaction): Future[(KeyValueObjectPointer, Future[SimpleTaskExecutor])] = {

    implicit val ec = client.clientContext

    for {
      executor <- executorAllocator.allocateKeyValueObject(revisionGuard, Map())
      ftree <- KVObjectRootManager.createNewTree(client, executor, TreeKey, TreeOrdering, nodeAllocator, Map())
    } yield {
      (executor, ftree.map(rootMgr => new SimpleTaskExecutor(client, executor, rootMgr)))
    }
  }
}

class SimpleTaskExecutor(val client: AmoebaClient,
                         val executorObject: KeyValueObjectPointer,
                         val root: KVObjectRootManager) extends TaskExecutor {


  override def prepareTask(taskType: DurableTaskType,
                           initialState: List[(Key, Array[Byte])])(implicit tx: Transaction): Future[Future[Option[AnyRef]]] = ???

  override def resume(): Unit = ???
}