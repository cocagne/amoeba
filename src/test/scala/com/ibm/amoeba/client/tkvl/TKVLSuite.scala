package com.ibm.amoeba.client.tkvl

import com.ibm.amoeba.IntegrationTestSuite
import com.ibm.amoeba.client.Transaction
import com.ibm.amoeba.common.Nucleus
import com.ibm.amoeba.common.ida.Replication
import com.ibm.amoeba.common.objects.{ByteArrayKeyOrdering, Key, ObjectRevisionGuard, Value}

class TKVLSuite extends IntegrationTestSuite {
  test("Create new tree") {
    val treeKey = Key(Array[Byte](1))
    val key = Key(Array[Byte](2))
    val value = Value(Array[Byte](3))

    implicit val tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(nucleus)
      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(nucleus, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Nucleus.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))

      _ <- tx1.commit()

      root <- froot
      tree <- root.getTree()

      v <- tree.get(key)

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (1)
      vs.value.bytes(0) should be (3)
    }
  }

  test("Insert into tree") {
    val treeKey = Key(Array[Byte](0))
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](3))

    val key2 = Key(Array[Byte](4))

    val value2 = Value(new Array[Byte](512*1024))

    implicit val tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(nucleus)
      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(nucleus, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Nucleus.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))

      _ <- tx1.commit()

      root <- froot
      tree <- root.getTree()

      tx = client.newTransaction()
      _ <- tree.set(key2, value2)(tx)
      r <- tx.commit()

      v <- tree.get(key2)

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (512*1024)
    }
  }

  test("Splitting tree insertion") {
    val treeKey = Key(Array[Byte](0))
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](3))

    val key2 = Key(Array[Byte](4))
    val value2 = Value(new Array[Byte](512*1024))

    val key3 = Key(Array[Byte](5))
    val value3 = Value(new Array[Byte](512*1024))

    implicit val tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(nucleus)
      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(nucleus, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Nucleus.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))

      _ <- tx1.commit()

      root <- froot
      tree <- root.getTree()

      tx = client.newTransaction()
      _ <- tree.set(key2, value2)(tx)
      r <- tx.commit()


      tx = client.newTransaction()
      _ <- tree.set(key3, value3)(tx)
      r <- tx.commit()

      _ <- waitForTransactionsToComplete()

      tree <- root.getTree()
      v <- tree.get(key3)
      (numTiers, _, _) <- tree.rootManager.getRootNode()

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (512*1024)
      numTiers should be (1)
    }
  }

  test("Joining tree deletion with tier reduction") {
    val treeKey = Key(Array[Byte](0))
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](3))

    val key2 = Key(Array[Byte](4))
    val value2 = Value(new Array[Byte](512*1024))

    val key3 = Key(Array[Byte](7))
    val value3 = Value(new Array[Byte](512*1024))

    implicit val tx1: Transaction = client.newTransaction()

    for {
      ikvos <- client.read(nucleus)
      pool <- client.getStoragePool(Nucleus.poolId)
      alloc = pool.createAllocater(Replication(3,2))

      ptr <- alloc.allocateKeyValueObject(ObjectRevisionGuard(nucleus, ikvos.revision), Map(), None, None, None)

      nodeAllocator = new SinglePoolNodeAllocator(client, Nucleus.poolId)

      froot <- KVObjectRootManager.createNewTree(client, ptr, treeKey, ByteArrayKeyOrdering, nodeAllocator, Map(key -> value))

      _ <- tx1.commit()

      root <- froot
      tree <- root.getTree()

      tx = client.newTransaction()
      _ <- tree.set(key2, value2)(tx)
      r <- tx.commit()

      tx = client.newTransaction()
      _ <- tree.set(key3, value3)(tx)
      r <- tx.commit()
      _=println("-- Pre Delete")
      tx = client.newTransaction()
      _ <- tree.delete(key2)(tx)
      _=println("Committing first delete")
      r <- tx.commit()
      _=println("-- Post Delete 1")
      tx = client.newTransaction()
      _ <- tree.delete(key)(tx)
      r <- tx.commit()
      _=println("-- Post Delete 2")
      _ <- waitForTransactionsToComplete()
      _=println("-- all tx complete")

      tree <- root.getTree()
      v <- tree.get(key3)
      (numTiers, _, _) <- tree.rootManager.getRootNode()

    } yield {
      v.isEmpty should be (false)
      val vs = v.get
      vs.value.bytes.length should be (512*1024)
      numTiers should be (1)
    }
  }
}
