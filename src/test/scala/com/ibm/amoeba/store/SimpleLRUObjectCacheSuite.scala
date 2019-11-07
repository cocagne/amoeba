package com.ibm.amoeba.store

import java.util.UUID

import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.objects.{Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import com.ibm.amoeba.common.store.{ObjectState, SimpleLRUObjectCache, StorePointer}
import com.ibm.amoeba.common.transaction.TransactionId
import org.scalatest.{FunSuite, Matchers}

object SimpleLRUObjectCacheSuite {

  def mkobjs(): (ObjectState, ObjectState, ObjectState, ObjectState) = {

    val o1 = new ObjectState(
      ObjectId(new UUID(0,1)),
      StorePointer(1, new Array[Byte](0)),
      Metadata(ObjectRevision(new UUID(0, 2)),
        ObjectRefcount(1,1),
        HLCTimestamp(1)),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
    )

    val o2 = new ObjectState(
      ObjectId(new UUID(0,2)),
      StorePointer(1, new Array[Byte](0)),
      Metadata(ObjectRevision(new UUID(0, 2)),
        ObjectRefcount(1,1),
        HLCTimestamp(1)),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
    )

    val o3 = new ObjectState(
      ObjectId(new UUID(0,3)),
      StorePointer(1, new Array[Byte](0)),
      Metadata(ObjectRevision(new UUID(0, 2)),
        ObjectRefcount(1,1),
        HLCTimestamp(1)),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
    )

    val o4 = new ObjectState(
      ObjectId(new UUID(0,4)),
      StorePointer(1, new Array[Byte](0)),
      Metadata(ObjectRevision(new UUID(0, 2)),
        ObjectRefcount(1,1),
        HLCTimestamp(1)),
      ObjectType.Data,
      DataBuffer(new Array[Byte](0)),
      None
    )

    (o1, o2, o3, o4)
  }
}

class SimpleLRUObjectCacheSuite extends FunSuite with Matchers {
  import SimpleLRUObjectCacheSuite._

  test("Max size") {
    val c = new SimpleLRUObjectCache(3)
    val (o1, o2, o3, o4) = mkobjs()

    assert(c.insert(o1).isEmpty)
    assert(c.insert(o2).isEmpty)
    assert(c.insert(o3).isEmpty)
    assert(c.insert(o4).contains(o1))
  }

  test("Get increases priority") {
    val c = new SimpleLRUObjectCache(3)
    val (o1, o2, o3, o4) = mkobjs()

    assert(c.insert(o1).isEmpty)
    assert(c.insert(o2).isEmpty)
    assert(c.insert(o3).isEmpty)

    assert(c.get(o1.objectId).contains(o1))
    assert(c.get(o2.objectId).contains(o2))

    assert(c.insert(o4).contains(o3))
  }

  test("Two element increases priority") {
    val c = new SimpleLRUObjectCache(3)
    val (o1, o2, o3, o4) = mkobjs()

    assert(c.insert(o1).isEmpty)
    assert(c.insert(o2).isEmpty)

    assert(c.get(o1.objectId).contains(o1))
    assert(c.insert(o3).isEmpty)
    assert(c.get(o2.objectId).contains(o2))

    assert(c.insert(o4).contains(o3))
  }

  test("Skip locked transactions") {
    val c = new SimpleLRUObjectCache(3)
    val (o1, o2, o3, o4) = mkobjs()

    assert(c.insert(o1).isEmpty)
    assert(c.insert(o2).isEmpty)
    assert(c.insert(o3).isEmpty)

    o1.transactionReferences = 1
    o2.transactionReferences = 2

    assert(c.insert(o4).contains(o3))
  }
}
