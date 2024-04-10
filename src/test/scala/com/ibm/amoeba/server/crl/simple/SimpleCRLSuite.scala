package com.ibm.amoeba.server.crl.simple

import com.ibm.amoeba.FileBasedTests
import com.ibm.amoeba.common.ida.Replication
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.objects.{DataObjectPointer, ObjectId, ObjectRefcount, ObjectRevision, ObjectType}
import com.ibm.amoeba.common.paxos.{PersistentState, ProposalId}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.{DataUpdate, DataUpdateOperation, ObjectUpdate, TransactionDescription, TransactionDisposition, TransactionId, TransactionStatus}
import com.ibm.amoeba.server.crl.{AllocationRecoveryState, TransactionRecoveryState}

import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue

object SimpleCRLSuite:
  val transactionId = TransactionId(new UUID(0, 1))
  val transactionId2 = TransactionId(new UUID(0, 3))
  val poolId = PoolId(new UUID(0, 2))
  val storeId = StoreId(poolId, 1)
  val storeId2 = StoreId(poolId, 2)

  val txid = TxId(storeId, transactionId)
  val txid2 = TxId(storeId2, transactionId2)

  val txdata = DataBuffer(Array[Byte](1, 2))
  val oud1 = DataBuffer(Array[Byte](3, 4))
  val oud2 = DataBuffer(Array[Byte](4))
  val ou1 = ObjectUpdate(ObjectId(new UUID(0, 3)), oud1)
  val ou2 = ObjectUpdate(ObjectId(new UUID(0, 4)), oud2)
  val disp = TransactionDisposition.VoteCommit
  val status = TransactionStatus.Unresolved
  val promise = ProposalId(1, 1)
  val accept = ProposalId(2, 2)
  val pax = PersistentState(Some(promise), Some((accept, true)))

  val storePointerEmpty = StorePointer(storeId.poolIndex, Array[Byte]())
  val storePointerEmpty2 = StorePointer(storeId2.poolIndex, Array[Byte]())
  val storePointerData = StorePointer(storeId.poolIndex, Array[Byte](1,2,3))
  val objectId = ObjectId(new UUID(0,5))
  val objectData = DataBuffer(Array[Byte](0,1))
  val objectSize = 5
  val refcount = ObjectRefcount(1,1)
  val timestamp = HLCTimestamp(2)
  val allocTxId = TransactionId(new UUID(0,6))
  val serializedRevisionGuard = DataBuffer(Array[Byte](0,1,2,3,4))
  val allocDataLocation = StreamLocation(StreamId(0), 5, 4)

  val trs = TransactionRecoveryState(storeId, txdata, List(ou1, ou2), disp, status, pax)
  val trs2 = TransactionRecoveryState(storeId2, txdata, Nil, disp, status, pax)

  val ars = AllocationRecoveryState(storeId, storePointerData, objectId, ObjectType.Data,
    Some(objectSize), objectData, refcount, timestamp , transactionId, serializedRevisionGuard)
  val ars2 = AllocationRecoveryState(storeId2, storePointerEmpty2, objectId, ObjectType.Data,
    None, objectData, refcount, timestamp, transactionId2, serializedRevisionGuard)

  val txdLoc = StreamLocation(StreamId(0), 16, 4)
  val ou1Loc = StreamLocation(StreamId(0), 25, 2)
  val ou2Loc = StreamLocation(StreamId(0), 27, 1)

  val updateLocations = Some((ou1.objectId, ou1Loc) :: (ou2.objectId, ou2Loc) :: Nil)

  val stream0 = StreamId(0)
  val stream1 = StreamId(1)
  val stream2 = StreamId(2)

  val oid1 = ObjectId(new UUID(0, 2))
  val op1 = new DataObjectPointer(oid1, poolId, None, Replication(3, 2), Array(storePointerEmpty))
  val txd = TransactionDescription(transactionId, timestamp, op1, 1.toByte,
    List(DataUpdate(op1, ObjectRevision(transactionId), DataUpdateOperation.Overwrite)),
    List(), None, List(), List())
  val trsValidTxd = TransactionRecoveryState(storeId, txd.serialize(), List(ou1, ou2), disp, status, pax)



class SimpleCRLSuite extends FileBasedTests {
  import SimpleCRLSuite._

  def streams(numStreams: Int): List[(StreamId, Path)] =
    var lst: List[(StreamId, Path)] = Nil
    for i <- 0 until numStreams do
      lst = (StreamId(i), tdir.toPath.resolve(f"$i.log")) :: lst
    lst.reverse


  test("SimpleCRL Drop Transaction Data Foo") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run

    i.crl.dropTransactionObjectData(storeId, transactionId)
    i.crl.save(transactionId, trsValidTxd, completionHandler)
    queue.take()

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 1024 * 1024)

    assert(i2.trsList.length == 1)
    assert(i2.arsList.length == 0)

    assert(i2.trsList.head.storeId == txid.storeId)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == List())
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  test("SimpleCRL Recycle Streams") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 4096 * 3)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)
    i.crl.deleteTransaction(storeId, transactionId2)
    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 4096 * 3)

    assert(i2.trsList.length == 1)
    assert(i2.arsList.length == 0)

    assert(i2.trsList.head.storeId == txid.storeId)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.txd.transactionId == transactionId)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  test("SimpleCRL Recovery From Multiple Streams") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 4096 * 3)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.deleteTransaction(storeId, transactionId2)
    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 2)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 4096 * 3)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)

    assert(i.crl.currentStreamNumber == 2)

    assert(i2.trsList.length == 1)
    assert(i2.arsList.length == 0)

    assert(i2.trsList.head.storeId == txid.storeId)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  test("SimpleCRL Switch Streams") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 4096*3)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 0)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take()
    assert(i.crl.currentStreamNumber == 1)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 4096*3)

    i.crl.save(transactionId2, trsValidTxd, completionHandler)

    assert(i.crl.currentStreamNumber == 1)

    assert(i2.trsList.length == 1)
    assert(i2.arsList.length == 0)

    assert(i2.trsList.head.storeId == txid.storeId)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  test("SimpleCRL Basic Functionality") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val i = SimpleCRL(tdir.toPath, 3, 1024*1024)

    i.crl.save(transactionId, trsValidTxd, completionHandler)

    queue.take() // Block till completion handlers are run

    assert(i.crl.currentEntrySerialNumber == 1)

    i.crl.shutdown()

    val i2 = SimpleCRL(tdir.toPath, 3, 1024*1024)

    assert(i2.crl.currentEntrySerialNumber == 2)

    assert(i2.trsList.length == 1)
    assert(i2.arsList.length == 0)

    assert(i2.trsList.head.storeId == txid.storeId)
    assert(i2.trsList.head.disposition == disp)
    assert(i2.trsList.head.serializedTxd == trsValidTxd.serializedTxd)
    assert(i2.trsList.head.objectUpdates == trsValidTxd.objectUpdates)
    assert(i2.trsList.head.paxosAcceptorState == pax)
  }

  test("Save & Recover with Tx in Multiple Log Entries") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val streamWriter = new StreamWriter(4096 * 1000, streams(1))

    val tx = Tx(txid, trs, None, None)

    val le = LogEntry(StreamLocation.Null, 0, 0)
    val stream = new Stream(stream0, streamWriter, UUID.randomUUID(), 0)

    le.addTx(tx, completionHandler)

    val entry1Location = stream.writeEntry(le, () => ())

    val le2 = LogEntry(entry1Location, 1, 0)

    le2.addTx(tx, completionHandler)

    val entry2Location = stream.writeEntry(le2, () => ())

    queue.take() // Block till completion handlers are run

    streamWriter.shutdown()

    val r = Recovery.recover(streams(1))

    assert(r.trsList.length == 1)
    assert(r.arsList.length == 0)
    assert(r.activeStreamId == stream0)

    assert(r.trsList.head.storeId == txid.storeId)
    assert(r.trsList.head.disposition == disp)
    assert(r.trsList.head.serializedTxd == trs.serializedTxd)
    assert(r.trsList.head.objectUpdates == trs.objectUpdates)
    assert(r.trsList.head.paxosAcceptorState == pax)
  }

  test("Save & Recover with Multiple Log Entries Tx & Alloc") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val streamWriter = new StreamWriter(4096 * 1000, streams(1))

    val tx = Tx(txid, trs, None, None)
    val tx2 = Tx(txid2, trs2, None, None)
    val alloc = Alloc(None, ars)
    val alloc2 = Alloc(None, ars2)
    val le = LogEntry(StreamLocation.Null, 0, 0)
    val stream = new Stream(stream0, streamWriter, UUID.randomUUID(), 0)

    le.addTx(tx, completionHandler)
    le.addAllocation(alloc, completionHandler)

    val entry1Location = stream.writeEntry(le, () => ())

    val le2 = LogEntry(entry1Location, 1, 0)

    le2.deleteTx(txid)
    le2.deleteAllocation(txid)
    le2.addTx(tx2, completionHandler)
    le2.addAllocation(alloc2, completionHandler)

    val entry2Location = stream.writeEntry(le2, () => ())

    queue.take() // Block till completion handlers are run

    streamWriter.shutdown()

    val r = Recovery.recover(streams(1))

    assert(r.trsList.length == 1)
    assert(r.arsList.length == 1)
    assert(r.activeStreamId == stream0)

    assert(r.trsList.head.storeId == txid2.storeId)
    assert(r.trsList.head.disposition == disp)
    assert(r.trsList.head.serializedTxd == trs2.serializedTxd)
    assert(r.trsList.head.objectUpdates == trs2.objectUpdates)
    assert(r.trsList.head.paxosAcceptorState == pax)

    val x = r.arsList.head
    assert(x.storeId == ars2.storeId)
    assert(x.storePointer == ars2.storePointer)
    assert(x.newObjectId == ars2.newObjectId)
    assert(x.objectSize == ars2.objectSize)
    assert(x.objectData == ars2.objectData)
    assert(x.initialRefcount == ars2.initialRefcount)
    assert(x.timestamp == ars2.timestamp)
    assert(x.allocationTransactionId == ars2.allocationTransactionId)
    assert(x.serializedRevisionGuard == ars2.serializedRevisionGuard)
  }

  test("Save & Recover with Multiple Tx & Alloc") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val streamWriter = new StreamWriter(4096 * 1000, streams(1))

    val tx = Tx(txid, trs, None, None)
    val tx2 = Tx(txid2, trs2, None, None)
    val alloc = Alloc(None, ars)
    val alloc2 = Alloc(None, ars2)
    val le = LogEntry(StreamLocation.Null, 0, 0)
    val stream = new Stream(stream0, streamWriter, UUID.randomUUID(), 0)

    le.addTx(tx, completionHandler)
    le.addAllocation(alloc, completionHandler)

    le.addTx(tx2, completionHandler)
    le.addAllocation(alloc2, completionHandler)

    val entryLocation = stream.writeEntry(le, () => ())

    queue.take() // Block till completion handlers are run

    streamWriter.shutdown()

    val r = Recovery.recover(streams(1))

    assert(r.trsList.length == 2)
    assert(r.arsList.length == 2)
    assert(r.activeStreamId == stream0)

    assert(r.trsList.head.storeId == txid.storeId)
    assert(r.trsList.head.disposition == disp)
    assert(r.trsList.head.serializedTxd == trs.serializedTxd)
    assert(r.trsList.head.objectUpdates == trs.objectUpdates)
    assert(r.trsList.head.paxosAcceptorState == pax)

    assert(r.trsList.tail.head.storeId == txid2.storeId)
    assert(r.trsList.tail.head.disposition == disp)
    assert(r.trsList.tail.head.serializedTxd == trs2.serializedTxd)
    assert(r.trsList.tail.head.objectUpdates == trs2.objectUpdates)
    assert(r.trsList.tail.head.paxosAcceptorState == pax)

    val x = r.arsList.head
    assert(x.storeId == ars.storeId)
    assert(x.storePointer == ars.storePointer)
    assert(x.newObjectId == ars.newObjectId)
    assert(x.objectSize == ars.objectSize)
    assert(x.objectData == ars.objectData)
    assert(x.initialRefcount == ars.initialRefcount)
    assert(x.timestamp == ars.timestamp)
    assert(x.allocationTransactionId == ars.allocationTransactionId)
    assert(x.serializedRevisionGuard == ars.serializedRevisionGuard)

    val x2 = r.arsList.tail.head
    assert(x2.storeId == ars2.storeId)
    assert(x2.storePointer == ars2.storePointer)
    assert(x2.newObjectId == ars2.newObjectId)
    assert(x2.objectSize == ars2.objectSize)
    assert(x2.objectData == ars2.objectData)
    assert(x2.initialRefcount == ars2.initialRefcount)
    assert(x2.timestamp == ars2.timestamp)
    assert(x2.allocationTransactionId == ars2.allocationTransactionId)
    assert(x2.serializedRevisionGuard == ars2.serializedRevisionGuard)
  }

  test("Alloc Save & Recover with Single Alloc") {
    val queue = new LinkedBlockingQueue[String]()

    def completionHandler(): Unit = queue.put("")

    val streamWriter = new StreamWriter(4096 * 1000, streams(1))

    val alloc = Alloc(None, ars)
    val le = LogEntry(StreamLocation.Null, 0, 0)
    val stream = new Stream(stream0, streamWriter, UUID.randomUUID(), 0)

    le.addAllocation(alloc, completionHandler)

    val entryLocation = stream.writeEntry(le, () => ())

    assert(entryLocation.streamId == stream0)
    assert(entryLocation.offset == 0)

    queue.take() // Block till completion handlers are run

    streamWriter.shutdown()

    val r = Recovery.recover(streams(1))

    assert(r.trsList.length == 0)
    assert(r.arsList.length == 1)
    assert(r.activeStreamId == stream0)

    val x = r.arsList.head
    assert(x.storeId == ars.storeId)
    assert(x.storePointer == ars.storePointer)
    assert(x.newObjectId == ars.newObjectId)
    assert(x.objectSize == ars.objectSize)
    assert(x.objectData == ars.objectData)
    assert(x.initialRefcount == ars.initialRefcount)
    assert(x.timestamp == ars.timestamp)
    assert(x.allocationTransactionId == ars.allocationTransactionId)
    assert(x.serializedRevisionGuard == ars.serializedRevisionGuard)
  }

  test("Tx Save & Recover with Single Transaction") {
    val queue = new LinkedBlockingQueue[String]()
    def completionHandler(): Unit = queue.put("")

    val streamWriter = new StreamWriter(4096*1000, streams(1))

    val tx = Tx(txid, trs, None, None)
    val le = LogEntry(StreamLocation.Null, 0, 0)
    val stream = new Stream(stream0, streamWriter, UUID.randomUUID(), 0)

    le.addTx(tx, completionHandler)

    val entryLocation = stream.writeEntry(le, () => ())

    assert(entryLocation.streamId == stream0)
    assert(entryLocation.offset == 0)

    queue.take() // Block till completion handlers are run

    streamWriter.shutdown()

    val r = Recovery.recover(streams(1))

    assert(r.trsList.length == 1)
    assert(r.arsList.length == 0)
    assert(r.activeStreamId == stream0)

    assert(r.trsList.head.storeId == txid.storeId)
    assert(r.trsList.head.disposition == disp)
    assert(r.trsList.head.serializedTxd == trs.serializedTxd)
    assert(r.trsList.head.objectUpdates == trs.objectUpdates)
    assert(r.trsList.head.paxosAcceptorState == pax)
  }

  test("Recovery without log files") {
    val r = Recovery.recover(Nil)

    assert(r.trsList.length == 0)
    assert(r.arsList.length == 0)
    assert(r.activeStreamId == stream0)
  }

  test("Alloc Static Save & Load With Empty StorePointer & Data") {
    val ars = AllocationRecoveryState( storeId, storePointerEmpty, objectId, ObjectType.KeyValue,
      None, objectData, refcount, timestamp, allocTxId, serializedRevisionGuard)

    val a = Alloc(Some(allocDataLocation), ars)

    val buffer = new Array[Byte](4096 * 5)

    assert(a.dynamicDataSize == 0)

    a.writeStaticEntry(ByteBuffer.wrap(buffer))

    val la = Alloc.loadAlloc(ByteBuffer.wrap(buffer))

    assert(la.txid.storeId == ars.storeId)
    assert(la.txid.transactionId == ars.allocationTransactionId)
    assert(la.storePointer == ars.storePointer)
    assert(la.newObjectId == ars.newObjectId)
    assert(la.objectType == ars.objectType)
    assert(la.objectSize == ars.objectSize)
    assert(la.initialRefcount == ars.initialRefcount)
    assert(la.timestamp == ars.timestamp)
    assert(la.serializedRevisionGuard == ars.serializedRevisionGuard)
  }

  test("Alloc Static Save & Load With NonEmpty StorePointer & No Data") {
    val noObjectData = DataBuffer(Array[Byte]())
    val ars = AllocationRecoveryState(storeId, storePointerData, objectId, ObjectType.Data,
      None, noObjectData, refcount, timestamp, allocTxId, serializedRevisionGuard)

    val a = Alloc(Some(allocDataLocation), ars)

    val buffer = new Array[Byte](4096 * 5)

    assert(a.dynamicDataSize == 0)

    a.writeStaticEntry(ByteBuffer.wrap(buffer))

    val la = Alloc.loadAlloc(ByteBuffer.wrap(buffer))

    assert(la.txid.storeId == ars.storeId)
    assert(la.txid.transactionId == ars.allocationTransactionId)
    assert(la.storePointer == ars.storePointer)
    assert(la.newObjectId == ars.newObjectId)
    assert(la.objectType == ars.objectType)
    assert(la.objectSize == ars.objectSize)
    assert(la.initialRefcount == ars.initialRefcount)
    assert(la.timestamp == ars.timestamp)
    assert(la.serializedRevisionGuard == ars.serializedRevisionGuard)
  }

  test("Tx Static Save & Load with ObjectUpdate data") {
    val tx = Tx(txid, trs, Some(txdLoc), updateLocations)

    val buffer = new Array[Byte](4096 * 5)

    assert(tx.dynamicDataSize == 0)

    tx.writeStaticEntry(ByteBuffer.wrap(buffer))

    val ltx = Tx.loadTx(ByteBuffer.wrap(buffer))

    assert(ltx.id == txid)
    assert(ltx.disposition == disp)
    assert(ltx.txdLocation == txdLoc)
    assert(ltx.updateLocations == updateLocations)
    assert(ltx.paxosAcceptorState == pax)
  }

  test("Tx Static Save & Load without ObjectUpdate data") {
    val tx = Tx(txid, trs, Some(txdLoc), None, keepObjectUpdates = false)

    val buffer = new Array[Byte](4096 * 5)

    assert(tx.dynamicDataSize == 0)

    tx.writeStaticEntry(ByteBuffer.wrap(buffer))

    val ltx = Tx.loadTx(ByteBuffer.wrap(buffer))

    assert(ltx.id == txid)
    assert(ltx.disposition == disp)
    assert(ltx.txdLocation == txdLoc)
    assert(ltx.updateLocations.isEmpty)
    assert(ltx.paxosAcceptorState == pax)
  }
}
