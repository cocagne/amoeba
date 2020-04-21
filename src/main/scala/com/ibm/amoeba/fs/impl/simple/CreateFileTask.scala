package com.ibm.amoeba.fs.impl.simple

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID

import com.ibm.amoeba.client.tkvl.TieredKeyValueList
import com.ibm.amoeba.client.{AmoebaClient, KeyValueObjectState, Transaction}
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, DataObjectPointer, Insert, Key, KeyRevisionGuard, ObjectRefcount, ObjectRevision, Value}
import com.ibm.amoeba.common.transaction.KeyValueUpdate.KeyRevision
import com.ibm.amoeba.compute.{DurableTask, DurableTaskPointer, DurableTaskType, TaskExecutor}
import com.ibm.amoeba.common.util.{byte2uuid, uuid2byte}
import com.ibm.amoeba.fs.{DirectoryInode, DirectoryPointer, FileSystem, Inode, InodePointer}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object CreateFileTask extends DurableTaskType {
  val typeUUID: UUID = UUID.fromString("48A4F255-7B78-4D7F-B8AB-D9301B8CDA40")

  private val FileSystemUUIDKey  = Key(1)
  private val DirectoryInodeKey  = Key(2)
  private val InodeKey           = Key(3)
  private val FileNameKey        = Key(4)
  private val NewFilePointerKey  = Key(5)
  private val StepKey            = Key(6)

  def createTask(client: AmoebaClient,
                 pointer: DurableTaskPointer,
                 revision: ObjectRevision,
                 state: Map[Key, KeyValueObjectState.ValueState]): DurableTask = {

    val fsUUID = byte2uuid(state(FileSystemUUIDKey).value.bytes)
    val ptr = InodePointer(state(DirectoryInodeKey).value.bytes).asInstanceOf[DirectoryPointer]
    val inode = Inode(client, state(InodeKey).value.bytes)
    val fileName = new String(state(FileNameKey).value.bytes, StandardCharsets.UTF_8)
    val fs = FileSystem.getRegisteredFileSystem(fsUUID).get

    new CreateFileTask(pointer, fs, ptr, fileName, inode)
  }

  def prepareTask(fileSystem: FileSystem,
                  taskExecutor: TaskExecutor,
                  directoryPointer: DirectoryPointer,
                  fileName: String,
                  inode: Inode)(implicit tx: Transaction): Future[Future[Option[AnyRef]]] = {
    val istate = List(
      FileSystemUUIDKey -> uuid2byte(fileSystem.uuid),
      DirectoryInodeKey -> directoryPointer.toArray,
      InodeKey -> inode.toArray,
      FileNameKey -> fileName.getBytes(StandardCharsets.UTF_8))
    taskExecutor.prepareTask(this, istate)
  }
}

class CreateFileTask(val taskPointer: DurableTaskPointer,
                     val fs: FileSystem,
                     val directoryPointer: DirectoryPointer,
                     val fileName: String,
                     val inode: Inode) extends DurableTask {

  import CreateFileTask._

  implicit val ec: ExecutionContext = fs.executionContext

  private val promise = Promise[Option[AnyRef]]()

  def completed: Future[Option[AnyRef]] = promise.future

  doNextStep()

  def doNextStep(): Unit = {
    for {
      kvos <- fs.client.read(taskPointer.kvPointer)
      step = kvos.contents(StepKey)
    } yield {
      step.value.bytes(0) match {
        case 0 => allocateInode(step.revision) onComplete {
          case Failure(_) => doNextStep()
          case Success(_) => doNextStep()
        }

        case 1 =>
          val newFilePointer = InodePointer(kvos.contents(NewFilePointerKey).value.bytes)
          addToDirectory(step.revision, newFilePointer) onComplete {
          case Failure(_) => doNextStep()
          case Success(_) => doNextStep()
        }

        case _ => synchronized {
          if (! promise.isCompleted) {
            promise.success(None)
          }
        }
      }
    }
  }

  def allocateInode(stepRevision: ObjectRevision): Future[Unit] = {

    implicit val tx: Transaction = fs.client.newTransaction()

    val guard = KeyRevisionGuard(taskPointer.kvPointer, StepKey, stepRevision)

    fs.inodeTable.prepareInodeAllocation(inode, guard).onComplete {
      case Failure(err) => tx.invalidateTransaction(err)

      case Success(iptr) =>
      val nextStep = Insert(StepKey, Array[Byte](1)) ::
        Insert(NewFilePointerKey, iptr.toArray) :: Nil

      tx.update(taskPointer.kvPointer, None, None, KeyRevision(StepKey, stepRevision) :: Nil, nextStep)

      tx.commit()
    }

    tx.result.map(_=>())
  }

  def addToDirectory(stepRevision: ObjectRevision, newFile: InodePointer): Future[Unit] = {
    implicit val tx: Transaction = fs.client.newTransaction()

    val nextStep = Insert(StepKey, Array[Byte](2)) :: Nil

    val rootMgr = new SimpleDirectoryRootManager(fs.client, directoryPointer.pointer)
    val tkvl = new TieredKeyValueList(fs.client, rootMgr)
    val fkey = Key(fileName)

    for {
      onode <- tkvl.getContainingNode(fkey)
      _ <- onode match {
        case None =>
          tkvl.set(fkey, Value(newFile.toArray), requirement = Some(Left(true)))
        case Some(node) => node.get(fkey) match {
          case None =>
            node.set(fkey, Value(newFile.toArray), requirement = Some(Left(true)))
          case Some(vs) =>
            node.set(fkey, Value(newFile.toArray), requirement = Some(Right(vs.revision)))
        }
      }
    } yield {
      tx.update(taskPointer.kvPointer, None, None, KeyRevision(StepKey, stepRevision) :: Nil, nextStep)

      tx.commit().map(_=>())
    }
  }
}