package com.ibm.amoeba.client

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, DataObjectPointer, KeyValueObjectPointer, KeyValueOperation}

import scala.concurrent.Future

trait ObjectAllocator {

  val client: AmoebaClient

  val maxObjectSize: Option[Int]

  val objectIDA: IDA

  val allocatorId: ObjectAllocatorId

  def allocateDataObject(revisionGuard: AllocationRevisionGuard,
                         initialContent: DataBuffer)(implicit t: Transaction): Future[DataObjectPointer]

  def allocateKeyValueObject(revisionGuard: AllocationRevisionGuard,
                             initialContent: List[KeyValueOperation])(implicit t: Transaction): Future[KeyValueObjectPointer]

}
