package com.ibm.amoeba.client.internal

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.ibm.amoeba.client.{DataObjectState, KeyValueObjectState, ObjectCache, ObjectState}
import com.ibm.amoeba.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectPointer}

import scala.concurrent.duration._

class SimpleObjectCache extends ObjectCache {

  private val cache: Cache[ObjectId,ObjectState] = Scaffeine()
    .expireAfterWrite(Duration(5, SECONDS))
    .maximumSize(50)
    .build[ObjectId, ObjectState]()

  def get(pointer: DataObjectPointer): Option[DataObjectState] = {
    cache.getIfPresent(pointer.id).map(_.asInstanceOf[DataObjectState])
  }

  def get(pointer: KeyValueObjectPointer): Option[KeyValueObjectState] = {
    cache.getIfPresent(pointer.id).map(_.asInstanceOf[KeyValueObjectState])
  }

  /** To be called ONLY by read drivers */
  private[client] def put(pointer: ObjectPointer, dos: ObjectState): Unit = cache.put(pointer.id, dos)
}
