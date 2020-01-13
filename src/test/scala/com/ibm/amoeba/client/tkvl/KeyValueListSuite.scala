package com.ibm.amoeba.client.tkvl

import com.ibm.amoeba.IntegrationTestSuite
import com.ibm.amoeba.common.Nucleus
import com.ibm.amoeba.common.ida.Replication
import com.ibm.amoeba.common.objects.{ByteArrayKeyOrdering, Key, Value}

class KeyValueListSuite extends IntegrationTestSuite {

  test("Insert into empty list") {
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](2))

    implicit val tx = client.newTransaction()

    for {
      ikvos <- client.read(nucleus)
      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      lst = new KeyValueListNode(client, nucleus, ByteArrayKeyOrdering, Key.AbsoluteMinimum, ikvos.revision,
        Map(), None)

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

      lst = new KeyValueListNode(client, nucleus, ByteArrayKeyOrdering, Key.AbsoluteMinimum, ikvos.revision,
        Map(), None)

      tx = client.newTransaction()
      _ <- lst.insert(key, value, 100, alloc)(tx)

      _ <- tx.commit().map(_=>())

      lst <- lst.refresh()

      tx2 = client.newTransaction()
      _ <- lst.insert(key2, value2, 100, alloc)(tx2)

      _ <- tx2.commit().map(_=>())

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

      lst = new KeyValueListNode(client, nucleus, ByteArrayKeyOrdering, Key.AbsoluteMinimum, ikvos.revision,
        Map(), None)

      tx = client.newTransaction()
      _ <- lst.insert(key, value, 100, alloc)(tx)

      _ <- tx.commit().map(_=>())

      lst <- lst.refresh()

      tx2 = client.newTransaction()
      _ <- lst.insert(key2, value2, 40, alloc)(tx2)

      _ <- tx2.commit().map(_=>())

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

      lst = new KeyValueListNode(client, nucleus, ByteArrayKeyOrdering, Key.AbsoluteMinimum, ikvos.revision,
        Map(), None)

      tx = client.newTransaction()
      _ <- lst.insert(key, value, 100, alloc)(tx)

      _ <- tx.commit().map(_=>())

      lst <- lst.refresh()

      tx2 = client.newTransaction()
      _ <- lst.insert(key2, value2, 40, alloc)(tx2)

      _ <- tx2.commit().map(_=>())

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

      lst = new KeyValueListNode(client, nucleus, ByteArrayKeyOrdering, Key.AbsoluteMinimum, ikvos.revision,
        Map(), None)

      tx = client.newTransaction()
      _ <- lst.insert(key, value, 100, alloc)(tx)

      _ <- tx.commit().map(_=>())

      lst <- lst.refresh()

      tx2 = client.newTransaction()
      _ <- lst.insert(key2, value2, 100, alloc)(tx2)

      _ <- tx2.commit().map(_=>())

      lst <- lst.refresh()

      tx3 = client.newTransaction()
      _ <- lst.insert(key3, value3, 60, alloc)(tx3)

      _ <- tx3.commit().map(_=>())

      lst <- lst.refresh()

      v <- lst.fetch(key)

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (1)
      vs.value.bytes(0) should be (2)
    }
  }
}
