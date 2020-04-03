package com.ibm.amoeba.client.tkvl

import com.ibm.amoeba.IntegrationTestSuite
import com.ibm.amoeba.common.Nucleus
import com.ibm.amoeba.common.ida.Replication
import com.ibm.amoeba.common.objects.{ByteArrayKeyOrdering, Key, ObjectRefcount, ObjectRevisionGuard, Value}

class KeyValueListSuite extends IntegrationTestSuite {

  test("Insert into empty list") {
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](2))

    implicit val tx = client.newTransaction()

    for {
      ikvos <- client.read(nucleus)

      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      lptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(nucleus, ikvos.revision), Map(), None, None, None)

      lst = new KeyValueListNode(client, lptr, ByteArrayKeyOrdering, Key.AbsoluteMinimum, tx.revision,
        ObjectRefcount(0,1), Map(), None)

      _ <- lst.insert(key, value, 100, alloc)

      _ <- tx.commit().map(_=>())

      lst <- lst.refresh()

      v <- lst.fetch(key)

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (1)
      vs.value.bytes(0) should be (2)
    }
  }

  test("Insert into empty non empty list") {
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](2))

    val key2 = Key(Array[Byte](0))
    val value2 = Value(Array[Byte](5))

    for {
      ikvos <- client.read(nucleus)
      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      tx = client.newTransaction()

      lptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(nucleus, ikvos.revision), Map(), None, None, None)(tx)

      lst = new KeyValueListNode(client, lptr, ByteArrayKeyOrdering, Key.AbsoluteMinimum, tx.revision,
        ObjectRefcount(0,1), Map(), None)

      _ <- lst.insert(key, value, 100, alloc)(tx)

      _ <- tx.commit().map(_=>())
      _ <- waitForTransactionsToComplete()

      lst <- lst.refresh()

      tx2 = client.newTransaction()
      _ <- lst.insert(key2, value2, 100, alloc)(tx2)

      _ <- tx2.commit().map(_=>())
      _ <- waitForTransactionsToComplete()

      lst <- lst.refresh()

      v <- lst.fetch(key2)

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (1)
      vs.value.bytes(0) should be (5)
    }
  }

  test("End split") {
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](2))

    val key2 = Key(Array[Byte](2))
    val value2 = Value(Array[Byte](5))

    for {
      ikvos <- client.read(nucleus)
      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      tx = client.newTransaction()

      lptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(nucleus, ikvos.revision), Map(), None, None, None)(tx)

      lst = new KeyValueListNode(client, lptr, ByteArrayKeyOrdering, Key.AbsoluteMinimum, tx.revision,
        ObjectRefcount(0,1), Map(), None)


      _ <- lst.insert(key, value, 100, alloc)(tx)

      _ <- tx.commit().map(_=>())
      _ <- waitForTransactionsToComplete()

      lst <- lst.refresh()

      tx2 = client.newTransaction()
      _ <- lst.insert(key2, value2, 40, alloc)(tx2)

      _ <- tx2.commit().map(_=>())
      _ <- waitForTransactionsToComplete()

      lst <- lst.refresh()

      v <- lst.fetch(key2)

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (1)
      vs.value.bytes(0) should be (5)
    }
  }

  test("Push out split") {
    val key = Key(Array[Byte](2))
    val value = Value(Array[Byte](2))

    val key2 = Key(Array[Byte](0))
    val value2 = Value(Array[Byte](5))

    for {
      ikvos <- client.read(nucleus)
      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      tx = client.newTransaction()

      lptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(nucleus, ikvos.revision), Map(), None, None, None)(tx)

      lst = new KeyValueListNode(client, lptr, ByteArrayKeyOrdering, Key.AbsoluteMinimum, tx.revision,
        ObjectRefcount(0,1), Map(), None)

      _ <- lst.insert(key, value, 100, alloc)(tx)

      _ <- tx.commit().map(_=>())
      _ <- waitForTransactionsToComplete()

      lst <- lst.refresh()

      tx2 = client.newTransaction()
      _ <- lst.insert(key2, value2, 40, alloc)(tx2)

      _ <- tx2.commit().map(_=>())
      _ <- waitForTransactionsToComplete()

      lst <- lst.refresh()

      v <- lst.fetch(key)

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (1)
      vs.value.bytes(0) should be (2)
    }
  }

  test("Multiple push out split") {
    val key = Key(Array[Byte](5))
    val value = Value(Array[Byte](2))

    val key2 = Key(Array[Byte](2))
    val value2 = Value(Array[Byte](5))

    val key3 = Key(Array[Byte](0))
    val value3 = Value(Array[Byte](5))

    for {
      ikvos <- client.read(nucleus)
      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      tx = client.newTransaction()

      lptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(nucleus, ikvos.revision), Map(), None, None, None)(tx)

      lst = new KeyValueListNode(client, lptr, ByteArrayKeyOrdering, Key.AbsoluteMinimum, tx.revision,
        ObjectRefcount(0,1), Map(), None)

      _ <- lst.insert(key, value, 100, alloc)(tx)

      _ <- tx.commit().map(_=>())
      _ <- waitForTransactionsToComplete()

      lst <- lst.refresh()

      tx2 = client.newTransaction()
      _ <- lst.insert(key2, value2, 100, alloc)(tx2)

      _ <- tx2.commit().map(_=>())
      _ <- waitForTransactionsToComplete()

      lst <- lst.refresh()

      tx3 = client.newTransaction()
      _ <- lst.insert(key3, value3, 60, alloc)(tx3)

      _ <- tx3.commit().map(_=>())
      _ <- waitForTransactionsToComplete()

      lst <- lst.refresh()

      v <- lst.fetch(key)

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (1)
      vs.value.bytes(0) should be (2)
    }
  }

  test("Join on delete to empty node") {
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](2))

    val key2 = Key(Array[Byte](2))
    val value2 = Value(Array[Byte](5))

    val key3 = Key(Array[Byte](3))
    val value3 = Value(Array[Byte](6))

    for {
      ikvos <- client.read(nucleus)
      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      tx = client.newTransaction()

      lptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(nucleus, ikvos.revision), Map(), None, None, None)(tx)

      lst = new KeyValueListNode(client, lptr, ByteArrayKeyOrdering, Key.AbsoluteMinimum, tx.revision,
        ObjectRefcount(0,1), Map(), None)

      _ <- lst.insert(key, value, 100, alloc)(tx)
      _ <- tx.commit().map(_=>())
      _ <- waitForTransactionsToComplete()
      lst <- lst.refresh()

      tx2 = client.newTransaction()
      _ <- lst.insert(key2, value2, 40, alloc)(tx2)
      _ <- tx2.commit().map(_=>())
      _ <- waitForTransactionsToComplete()
      lst <- lst.refresh()

      tx3 = client.newTransaction()
      _ <- lst.insert(key3, value3, 200, alloc)(tx3)
      _ <- tx3.commit().map(_=>())
      _ <- waitForTransactionsToComplete()
      lst <- lst.refresh()

      preNuc <- client.read(lptr)

      tx4 = client.newTransaction()
      _ <- lst.delete(key)(tx4)
      _ <- tx4.commit().map(_=>())
      _ <- waitForTransactionsToComplete()
      lst <- lst.refresh()

      postNuc <- client.read(lptr)

      v <- lst.fetch(key)
      v2 <- lst.fetch(key2)
      v3 <- lst.fetch(key3)

    } yield {
      v.isEmpty should be (true)

      v2.isEmpty should be (false)
      val vs2 = v2.get
      vs2.value.bytes.length should be (1)
      vs2.value.bytes(0) should be (5)

      v3.isEmpty should be (false)
      val vs3 = v3.get
      vs3.value.bytes.length should be (1)
      vs3.value.bytes(0) should be (6)

      preNuc.right.isEmpty should be (false)
      postNuc.right.isEmpty should be (true)

    }
  }

  test("Simple Delete") {
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](2))

    val key2 = Key(Array[Byte](2))
    val value2 = Value(Array[Byte](5))

    val key3 = Key(Array[Byte](3))
    val value3 = Value(Array[Byte](6))

    for {
      ikvos <- client.read(nucleus)
      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      tx = client.newTransaction()

      lptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(nucleus, ikvos.revision), Map(), None, None, None)(tx)

      lst = new KeyValueListNode(client, lptr, ByteArrayKeyOrdering, Key.AbsoluteMinimum, tx.revision,
        ObjectRefcount(0,1), Map(), None)

      _ <- lst.insert(key, value, 100, alloc)(tx)
      _ <- tx.commit().map(_=>())
      _ <- waitForTransactionsToComplete()
      lst <- lst.refresh()

      tx2 = client.newTransaction()
      _ <- lst.insert(key2, value2, 100, alloc)(tx2)
      _ <- tx2.commit().map(_=>())
      _ <- waitForTransactionsToComplete()
      lst <- lst.refresh()

      tx3 = client.newTransaction()
      _ <- lst.delete(key)(tx3)
      _ <- tx3.commit().map(_=>())
      lst <- lst.refresh()

      v <- lst.fetch(key)
      v2 <- lst.fetch(key2)

    } yield {
      v.isEmpty should be (true)

      v2.isEmpty should be (false)
      val vs2 = v2.get
      vs2.value.bytes.length should be (1)
      vs2.value.bytes(0) should be (5)
    }
  }
}
