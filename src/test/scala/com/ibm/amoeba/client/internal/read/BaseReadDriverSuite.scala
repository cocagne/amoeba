package com.ibm.amoeba.client.internal.read

import java.util.UUID

import com.ibm.amoeba.client.internal.OpportunisticRebuildManager
import com.ibm.amoeba.client.internal.allocation.AllocationManager
import com.ibm.amoeba.client.internal.network.Messenger
import com.ibm.amoeba.client.{AmoebaClient, CorruptedObject, DataObjectState, InvalidObject, KeyValueObjectState, ObjectCache, RetryStrategy, StoragePool, Transaction, TransactionFinalizer, TransactionStatusCache, TypeRegistry}
import com.ibm.amoeba.common.network.{ClientId, ClientResponse, ReadResponse}
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.ida.Replication
import com.ibm.amoeba.common.objects.{DataObjectPointer, KeyValueObjectPointer, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ReadError}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.{TransactionDescription, TransactionId}
import com.ibm.amoeba.common.util.BackgroundTask
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object BaseReadDriverSuite {
  val awaitDuration = Duration(100, MILLISECONDS)

  val objId = ObjectId(new UUID(0,1))
  val poolId = PoolId(new UUID(0,2))
  val readUUID = new UUID(0,3)
  val cliUUID = new UUID(0,4)

  val ida = Replication(3,2)

  val ds0 = StoreId(poolId, 0)
  val ds1 = StoreId(poolId, 1)
  val ds2 = StoreId(poolId, 2)

  val sp0 = StorePointer(0, List[Byte](0).toArray)
  val sp1 = StorePointer(1, List[Byte](1).toArray)
  val sp2 = StorePointer(2, List[Byte](2).toArray)

  val ptr = DataObjectPointer(objId, poolId, None, ida, (sp0 :: sp1 :: sp2 :: Nil).toArray)
  val kvptr = KeyValueObjectPointer(objId, poolId, None, ida, (sp0 :: sp1 :: sp2 :: Nil).toArray)
  val rev = ObjectRevision.Null
  val ref = ObjectRefcount(1,1)

  val odata = DataBuffer(List[Byte](1,2,3,4).toArray)

  val noLocks = Some(Map[StoreId, List[TransactionDescription]]())

  val client = ClientId(cliUUID)

  class TClient(override val clientId: ClientId) extends AmoebaClient {

    val txStatusCache: TransactionStatusCache = TransactionStatusCache.NoCache

    val typeRegistry: TypeRegistry = null

    def read(pointer: DataObjectPointer): Future[DataObjectState] = Future.failed(new Exception("TODO"))
    def read(pointer: KeyValueObjectPointer): Future[KeyValueObjectState] = Future.failed(new Exception("TODO"))

    def newTransaction(): Transaction = null

    def getStoragePool(poolId: PoolId): Future[StoragePool] = null

    val retryStrategy: RetryStrategy = null

    private[client] def backgroundTasks: BackgroundTask = BackgroundTask.NoBackgroundTasks

    private[client] def clientContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    private[client] def opportunisticRebuildManager: OpportunisticRebuildManager = OpportunisticRebuildManager.None

    private[client] val messenger: Messenger = Messenger.None

    private[client] val allocationManager: AllocationManager = null

    private[client] val objectCache: ObjectCache = ObjectCache.NoCache

    private[amoeba] def receiveClientResponse(msg: ClientResponse): Unit = ()

    private[amoeba] def getSystemAttribute(key: String): Option[String] = None
    private[amoeba] def setSystemAttribute(key: String, value: String): Unit = ()

    //private[amoeba] def createFinalizerFor(txd: TransactionDescription): TransactionFinalizer = null
  }

}

class BaseReadDriverSuite  extends AsyncFunSuite with Matchers {
  import BaseReadDriverSuite._


  def mkReader(client: AmoebaClient,
               objectPointer: ObjectPointer = ptr,
               readUUID:UUID = readUUID,
               disableOpportunisticRebuild: Boolean = false) = {
    new BaseReadDriver(client, objectPointer, readUUID, disableOpportunisticRebuild) {
      implicit protected val ec: ExecutionContext = client.clientContext
    }
  }

  test("Fail with invalid object") {
    val m = new TClient(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))

    val ts = HLCTimestamp.now
    val readTime = HLCTimestamp(ts.asLong - 100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, readTime, Left(ReadError.ObjectNotFound)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, readTime, Left(ReadError.ObjectMismatch)))

    r.readResult.isCompleted should be (true)

    recoverToSucceededIf[InvalidObject] {
      r.readResult
    }
  }

  test("Fail with corrupted object") {
    val m = new TClient(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))

    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Left(ReadError.CorruptedObject)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, Left(ReadError.CorruptedObject)))

    r.readResult.isCompleted should be (true)

    recoverToSucceededIf[CorruptedObject] {
      r.readResult
    }
  }

  test("Succeed with errors") {
    val m = new TClient(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now
    val readTime = HLCTimestamp(ts.asLong - 100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, readTime, Left(ReadError.ObjectNotFound)))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, readTime, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, readTime, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    //    o match {
    //      case Left(_) =>
    //      case Right((ds:DataObjectState, o)) =>
    //        println(s"ptr(${ds.pointer}), rev(${ds.revision}), ref(${ds.refcount}), ts(${ds.timestamp}), data(${com.ibm.aspen.util.db2string(ds.data)})")
    //        println(s"ptr(${ptr}), rev(${nrev2}), ref(${ref}), ts(${ts}), data(${com.ibm.aspen.util.db2string(odata)})")
    //    }

    o should be (DataObjectState(ptr, nrev2, ref, ts, readTime, 5, odata))
  }

  test("Ignore old revisions") {
    val m = new TClient(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev,   ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(nrev,  ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, nrev2, ref, ts, ts, 5, odata))
  }

  test("Use minimum readTime") {
    val m = new TClient(client)
    val r = mkReader(m)
    val nrev = ObjectRevision(TransactionId(new UUID(0,1)))
    val nrev2 = ObjectRevision(TransactionId(new UUID(0,2)))
    val ts = HLCTimestamp.now

    val minTs = HLCTimestamp(ts.asLong-100)

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev,   ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(nrev,  ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds2, readUUID, minTs, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(nrev2, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, nrev2, ref, ts, minTs, 5, odata))
  }


  test("Successful read with data and locks") {
    val m = new TClient(client)
    val r = mkReader(m)
    val ts = HLCTimestamp.now

    r.receiveReadResponse(ReadResponse(client, ds0, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (false)
    r.receiveReadResponse(ReadResponse(client, ds1, readUUID, ts, Right(ReadResponse.CurrentState(rev, ref, ts, 5, Some(odata), Set()))))
    r.readResult.isCompleted should be (true)
    val o = Await.result(r.readResult, awaitDuration)

    o should be (DataObjectState(ptr, rev, ref, ts, ts, 5, odata))
  }

}
