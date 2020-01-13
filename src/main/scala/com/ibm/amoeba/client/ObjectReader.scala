package com.ibm.amoeba.client

import com.ibm.amoeba.common.objects.{DataObjectPointer, KeyValueObjectPointer}

import scala.concurrent.Future

trait ObjectReader {

  def client: AmoebaClient

  def read(pointer: DataObjectPointer): Future[DataObjectState]

  def read(pointer: KeyValueObjectPointer): Future[KeyValueObjectState]
}
