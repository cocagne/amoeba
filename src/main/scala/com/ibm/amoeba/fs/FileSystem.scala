package com.ibm.amoeba.fs

import com.ibm.amoeba.client.{AmoebaClient, ObjectAllocator, ObjectAllocatorId}

import scala.concurrent.{ExecutionContext, Future}

trait FileSystem {
  private[fs] def client: AmoebaClient
  private[fs] def executionContext: ExecutionContext
  private[fs] def getObjectAllocator(id: ObjectAllocatorId): Future[ObjectAllocator]
}
