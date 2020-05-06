package com.ibm.amoeba.fs.impl.simple

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import com.ibm.amoeba.client.internal.allocation.SinglePoolObjectAllocator
import com.ibm.amoeba.client.tkvl.{KVObjectRootManager, NodeAllocator, Root, SinglePoolNodeAllocator}
import com.ibm.amoeba.client.{AmoebaClient, ExponentialBackoffRetryStrategy, KeyValueObjectState, ObjectAllocator, ObjectAllocatorId, Transaction}
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, IntegerKeyOrdering, Key, KeyValueObjectPointer, LexicalKeyOrdering, Value}
import com.ibm.amoeba.common.util.{byte2uuid, uuid2byte}
import com.ibm.amoeba.compute.TaskExecutor
import com.ibm.amoeba.compute.impl.SimpleTaskExecutor
import com.ibm.amoeba.fs.{DirectoryInode, DirectoryPointer, FileMode, FileSystem}

import scala.concurrent.{ExecutionContext, Future}

object SimpleFileSystem {

  private val FileSystemUUIDKey    = Key(1)
  private val TaskExecutorRootKey  = Key(2)
  private val InodeTableRootKey    = Key(3)

  def bootstrap(client: AmoebaClient,
                guard: AllocationRevisionGuard,
                allocator: ObjectAllocator): Future[FileSystem] = {

    implicit val ec: ExecutionContext = client.clientContext
    implicit val tx: Transaction = client.newTransaction()

    val uuid: UUID = UUID.randomUUID()

    val rootDirMode = FileMode.S_IFDIR | FileMode.S_IRWXU

    for {
      taskRoot <- allocator.allocateKeyValueObject(guard, Map())
      rootRoot = new Root(0, LexicalKeyOrdering, None, new SinglePoolNodeAllocator(client, taskRoot.poolId))
      rootDirInode = DirectoryInode.init(rootDirMode, 0, 0, None, Some(1), rootRoot)
      rootDirectory <- allocator.allocateDataObject(guard, rootDirInode.toArray)
      rootDirectoryPointer = new DirectoryPointer(1, rootDirectory)
      inodeTableContentRoot <- allocator.allocateKeyValueObject(guard, Map(Key(1) -> Value(rootDirectoryPointer.toArray)))
      inodeTableRoot = new Root(0, IntegerKeyOrdering, Some(inodeTableContentRoot), new SinglePoolNodeAllocator(client, taskRoot.poolId) )
      content = Map( FileSystemUUIDKey -> Value(uuid2byte(uuid)),
        TaskExecutorRootKey -> Value(taskRoot.toArray),
        InodeTableRootKey -> Value(inodeTableRoot.encode))
      fsRootPointer <- allocator.allocateKeyValueObject(guard, content)
      _=tx.overwrite(rootDirectory, tx.revision, rootDirInode.toArray) // ensure Tx has an object to modify
      _ <- tx.commit()
      fs <- load(client, fsRootPointer, numContextThreads = 4)
    } yield {
      fs
    }
  }

  def load(client: AmoebaClient,
           fsRoot: KeyValueObjectPointer,
           numContextThreads: Int): Future[SimpleFileSystem] = {
    implicit val ec: ExecutionContext = client.clientContext

    for {
      kvos <- client.read(fsRoot)
      rootPool <- client.getStoragePool(kvos.pointer.poolId)
      defaultAllocator = new SinglePoolObjectAllocator(client, rootPool, fsRoot.ida, None)
      executorRoot = KeyValueObjectPointer(kvos.contents(TaskExecutorRootKey).value.bytes)
      executor <- SimpleTaskExecutor(client, StaticTaskTypeRegistry.registeredTasks, defaultAllocator, executorRoot)
    } yield {
      new SimpleFileSystem(client, kvos, defaultAllocator, executor, numContextThreads)
    }

  }
}
class SimpleFileSystem(aclient: AmoebaClient,
                       fsRoot: KeyValueObjectState,
                       defaultAllocator: ObjectAllocator,
                       executor: TaskExecutor,
                       val numContextThreads: Int = 4) extends FileSystem {

  import SimpleFileSystem._

  private val sched = Executors.newScheduledThreadPool(numContextThreads)

  override val uuid: UUID =  byte2uuid(fsRoot.contents(FileSystemUUIDKey).value.bytes)

  override def shutdown(): Unit = {
    sched.shutdown()
    sched.awaitTermination(5, TimeUnit.SECONDS)
  }

  override private[fs] def retryStrategy = new ExponentialBackoffRetryStrategy(client)

  override private[fs] def taskExecutor = executor

  override private[fs] def defaultInodeAllocater = defaultAllocator

  override private[fs] def client = aclient

  override private[fs] val executionContext = ExecutionContext.fromExecutorService(sched)

  override private[fs] def getObjectAllocator(id: ObjectAllocatorId) = Future.successful(defaultAllocator)

  override private[fs] def inodeTable = new SimpleInodeTable(this, defaultAllocator,
    new KVObjectRootManager(client, InodeTableRootKey, fsRoot.pointer))

  FileSystem.register(this)
}
