package com.ibm.amoeba.client

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, DataObjectPointer, Key, KeyValueObjectPointer, KeyValueOperation, ObjectRefcount, Value}

import scala.concurrent.Future

trait ObjectAllocator {

  val maxObjectSize: Option[Int]

  val objectIDA: IDA

  def allocateDataObject(revisionGuard: AllocationRevisionGuard,
                         initialContent: DataBuffer,
                         initialRefcount: ObjectRefcount = ObjectRefcount(0,1))(implicit t: Transaction): Future[DataObjectPointer]

  def allocateKeyValueObject(revisionGuard: AllocationRevisionGuard,
                             initialContent: Map[Key,Value],
                             minimum: Option[Key] = None,
                             maximum: Option[Key] = None,
                             left: Option[Value] = None,
                             right: Option[Value] = None,
                             initialRefcount: ObjectRefcount = ObjectRefcount(0,1))(implicit t: Transaction): Future[KeyValueObjectPointer]

}
