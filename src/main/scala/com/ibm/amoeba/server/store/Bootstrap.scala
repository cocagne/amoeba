package com.ibm.amoeba.server.store

import java.util.UUID

import com.ibm.amoeba.client.StoragePool
import com.ibm.amoeba.client.internal.pool.SimpleStoragePool
import com.ibm.amoeba.client.tkvl.{BootstrapPoolNodeAllocator, Root}
import com.ibm.amoeba.common.{HLCTimestamp, Nucleus}
import com.ibm.amoeba.common.ida.IDA
import com.ibm.amoeba.common.objects.{ByteArrayKeyOrdering, Insert, Key, KeyValueObjectPointer, KeyValueOperation, Metadata, ObjectId, ObjectRefcount, ObjectRevision, ObjectType, Value}
import com.ibm.amoeba.common.transaction.TransactionId
import com.ibm.amoeba.server.store.backend.Backend

object Bootstrap {

    def initialize(ida: IDA, stores: List[Backend]): KeyValueObjectPointer = {
      require( ida.width == stores.length )

      val bootstrapMetadata = Metadata(
        ObjectRevision(TransactionId(new UUID(0,0))),
        ObjectRefcount(1,1),
        HLCTimestamp.now
      )

      def allocate(content: List[(Key, Array[Byte])] = Nil,
                   objectId: Option[ObjectId] = None): KeyValueObjectPointer = {

        val oid = objectId.getOrElse(ObjectId(UUID.randomUUID()))

        val contents = content.map { t =>
          t._1 -> new ValueState(Value(t._2), bootstrapMetadata.revision, bootstrapMetadata.timestamp, None)
        }.toMap

        val storePointers = KVObjectState.encodeIDA(ida, None, None, None, None, contents).zip(stores).map { t =>
          val (storeData, store) = t

          store.bootstrapAllocate(oid, ObjectType.KeyValue, bootstrapMetadata, storeData)
        }

        KeyValueObjectPointer(oid, Nucleus.poolId, None, ida, storePointers)
      }

      def overwrite(pointer: KeyValueObjectPointer,
                    content: List[(Key, Array[Byte])]): Unit = {
        val contents = content.map { t =>
          t._1 -> new ValueState(Value(t._2), bootstrapMetadata.revision, bootstrapMetadata.timestamp, None)
        }.toMap

        KVObjectState.encodeIDA(ida, None, None, None, None, contents).zip(stores).foreach { t =>
          val (storeData, store) = t
          val sp = pointer.getStorePointer(store.storeId).get

          store.bootstrapOverwrite(pointer.id, sp, storeData)
        }
      }

      val errTreeRoot = allocate()
      val allocTreeRoot = allocate()

      val poolConfig = SimpleStoragePool.encode(Nucleus.poolId, ida.width, ida, None, None)
      val errorTree = Root(0, ByteArrayKeyOrdering, Some(errTreeRoot), BootstrapPoolNodeAllocator).encode()
      val allocTree = Root(0, ByteArrayKeyOrdering, Some(allocTreeRoot), BootstrapPoolNodeAllocator).encode()

      val pool = allocate(List(StoragePool.ConfigKey -> poolConfig,
                               StoragePool.ErrorTreeKey -> errorTree,
                               StoragePool.AllocationTreeKey -> allocTree))

      val poolTreeRoot = allocate(List(Key(Nucleus.poolId.uuid) -> pool.toArray))
      val poolTree = Root(0,
        ByteArrayKeyOrdering,
        Some(poolTreeRoot),
        BootstrapPoolNodeAllocator)

      val nucleusContent: List[(Key, Array[Byte])] = List(Nucleus.PoolTreeKey -> poolTree.encode())

      val nucleus = allocate(nucleusContent, Some(Nucleus.objectId))

      overwrite(allocTreeRoot, List(
        Key(errTreeRoot.id.uuid) -> errTreeRoot.toArray,
        Key(allocTreeRoot.id.uuid) -> allocTreeRoot.toArray,
        Key(pool.id.uuid) -> pool.toArray,
        Key(poolTreeRoot.id.uuid) -> poolTreeRoot.toArray,
        Key(nucleus.id.uuid) -> nucleus.toArray
      ))

      if false then
        println(s"ErrorTreeRoot: ${errTreeRoot.id.uuid}")
        println(s"AllocTreeRoot: ${allocTreeRoot.id.uuid}")
        println(s"Nucleus Pool : ${pool.id.uuid}")
        println(s"PoolTreeRoot : ${poolTreeRoot.id.uuid}")
        println(s"Nucleus      : ${nucleus.id.uuid}")

      nucleus
    }



}
