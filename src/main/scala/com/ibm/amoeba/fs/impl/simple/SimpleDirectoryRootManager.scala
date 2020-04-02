package com.ibm.amoeba.fs.impl.simple

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

import com.ibm.amoeba.client.tkvl._
import com.ibm.amoeba.client.{AmoebaClient, ObjectAllocator, RegisteredTypeFactory, Transaction}
import com.ibm.amoeba.common.objects._
import com.ibm.amoeba.fs.DirectoryInode

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class SimpleDirectoryRootManager(client: AmoebaClient,
                                 inodePointer: DataObjectPointer) extends RootManager {

  import SimpleDirectoryRootManager._

  implicit val ec: ExecutionContext = client.clientContext

  override def typeId: RootManagerTypeId = RootManagerTypeId(typeUUID)

  private def getRoot(): Future[RData] = {
    val p = Promise[RData]()

    client.read(inodePointer).onComplete {
      case Failure(err) => p.failure(err)
      case Success(inodeDos) =>
        val inode = DirectoryInode(client, inodeDos.data)
        val root = inode.contents
        try {

          client.read(root.rootObject).onComplete {
            case Failure(err) => p.failure(err)
            case Success(rootKvos) =>

              val rootLp = KeyValueListPointer(Key.AbsoluteMinimum, root.rootObject)
              val node = KeyValueListNode(client, rootLp, root.ordering, rootKvos)

              p.success(RData(root, inodeDos.revision, node))
          }
        } catch {
          case _: Throwable => p.failure(new InvalidRoot)
        }

    }

    p.future
  }

  def getTree(): Future[TieredKeyValueList] = getRoot().map { rd =>
    new TieredKeyValueList(client, this)
  }

  def getAllocatorForTier(tier: Int): Future[ObjectAllocator] = getRoot().flatMap { rd =>
    rd.root.nodeAllocator.getAllocatorForTier(tier)
  }

  def getRootNode(): Future[(Int, KeyOrdering, KeyValueListNode)] = getRoot().map { rd =>
    (rd.root.tier, rd.root.ordering, rd.node)
  }

  def getMaxNodeSize(tier: Int): Future[Int] = getRoot().map { rd =>
    rd.root.nodeAllocator.getMaxNodeSize(tier)
  }

  override def getRootRevisionGuard(): Future[AllocationRevisionGuard] = {

    getRoot().map { rd =>
      ObjectRevisionGuard(rd.node.pointer, rd.rootRevision)
    }
  }

  override def encode(): Array[Byte] = {
    val arr = new Array[Byte]( inodePointer.encodedSize )
    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    inodePointer.encodeInto(bb)
    arr
  }

  override def prepareRootUpdate(newTier: Int,
                                 newRoot: KeyValueObjectPointer)(implicit tx: Transaction): Future[Unit] = {
    val p = Promise[Unit]()

    client.read(inodePointer).onComplete {
      case Failure(err) => p.failure(err)
      case Success(inodeDos) =>
        val inode = DirectoryInode(client, inodeDos.data)

        val root = inode.contents

        val nextRoot = root.copy(tier = newTier, rootObject = newRoot)
        val newInode = inode.setContentTree(nextRoot)

        tx.overwrite(inodePointer, inodeDos.revision, newInode.toArray)
        p.success(())
    }

    p.future
  }
}

object SimpleDirectoryRootManager extends RegisteredTypeFactory with RootManagerFactory {
  val typeUUID: UUID = UUID.fromString("52887CBE-0D2B-43C8-80FA-999DA177392D")

  private case class RData(root: Root, rootRevision: ObjectRevision, node: KeyValueListNode)

  def apply(client: AmoebaClient, bb: ByteBuffer): SimpleDirectoryRootManager = {
    new SimpleDirectoryRootManager(client, DataObjectPointer(bb))
  }

  override def createRootManager(client: AmoebaClient, data: Array[Byte]): SimpleDirectoryRootManager = {
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.BIG_ENDIAN)
    SimpleDirectoryRootManager(client, bb)
  }
}
