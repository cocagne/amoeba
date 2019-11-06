package com.ibm.amoeba.server.crl.sweeper

import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.objects.{ObjectId, ObjectRefcount, ObjectType}
import com.ibm.amoeba.common.paxos.{PersistentState, ProposalId}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.{ObjectUpdate, TransactionDisposition, TransactionId, TransactionStatus}
import com.ibm.amoeba.server.crl.{AllocationRecoveryState, CrashRecoveryLog, CrashRecoveryLogClient, CrashRecoveryLogFactory, SaveCompletion, SaveCompletionHandler, TransactionRecoveryState}
import org.apache.logging.log4j.scala.Logging

import scala.collection.immutable.{HashMap, HashSet}

object Sweeper {
  def createLogFile(directory: Path, fileId: FileId, maxSize: Long): LogFile = {
    new LogFile(directory.resolve(s"${fileId.number}"), fileId, maxSize)
  }

  class RecoveringState {
    var transactions: HashMap[TxId, Tx] = new HashMap
    var allocations: HashMap[TxId, Alloc] = new HashMap
    var deletedTx: HashSet[TxId] = new HashSet()
    var deletedAlloc: HashSet[TxId] = new HashSet()
    var lastEntry: Option[(LogEntrySerialNumber, FileLocation)] = None
  }

  def getFileLocation(bb: ByteBuffer): FileLocation = {
    val fileId = FileId(bb.getShort())
    val offset = bb.getLong()
    val size = bb.getInt()
    FileLocation(fileId, offset, size)
  }

  def getUUID(bb:ByteBuffer): UUID = {
    val msb = bb.getLong()
    val lsb = bb.getLong()
    new UUID(msb, lsb)
  }

  def getTxId(bb: ByteBuffer): TxId = {
    val poolId = PoolId(getUUID(bb))
    val poolIndex = bb.get()
    val txId = TransactionId(getUUID(bb))
    TxId(StoreId(poolId, poolIndex), txId)
  }

  object ExitThread extends Throwable
}

class Sweeper(directory: Path,
              numStreams: Int,
              maxFileSize: Long,
              maxEarliestWindow: Int)

  extends Logging with CrashRecoveryLogFactory {

  import Sweeper._

  private val files = new Array[LogFile](numStreams * 3)

  for (i <- files.indices)
    files(i) = createLogFile(directory, FileId(i), maxFileSize)

  private val streams = new Array[TriFileStream](numStreams)

  for (i <- streams.indices)
    streams(i) = new TriFileStream(maxFileSize, files(i), files(i+1), files(i+2))

  val contentQueue = new LogContentQueue

  private var (transactions, allocations, nextEntrySerialNumber, lastEntryLocation) = {
    val rs = recover()
    val (last, lastLoc) = rs.lastEntry.getOrElse((LogEntrySerialNumber(0), FileLocation.Null))
    val lastSerial = LogEntrySerialNumber(last.number + 1)
    val ilc = rs.transactions.valuesIterator ++ rs.allocations.valuesIterator

    ilc.toList.sorted.foreach { c => contentQueue.add(c) }

    (rs.transactions, rs.allocations, lastSerial, lastLoc)
  }

  private val queue = new LinkedBlockingQueue[Request]()
  private var nextRequest: Option[Request] = None
  private var clients = new HashMap[CrashRecoveryLogClient, SaveCompletionHandler]()
  private var nextClient: Int = 0

  private var pendingNotifications = new HashMap[LogEntrySerialNumber, List[SaveCompletion]]
  private var lastNotified: LogEntrySerialNumber = LogEntrySerialNumber(nextEntrySerialNumber.number-1)

  private val threadPool = Executors.newFixedThreadPool(numStreams)

  for (i <- streams.indices) {
    val stream = streams(i)
    threadPool.submit(new Runnable { override def run(): Unit = ioThread(stream) })
  }

  def shutdown(): Unit = {
    for (_ <- streams.indices)
      enqueue(new ExitIOThread)
    threadPool.shutdown()
    threadPool.awaitTermination(5, TimeUnit.SECONDS)
  }

  private def earliestNeeded = contentQueue.tail match {
    case None => LogEntrySerialNumber(nextEntrySerialNumber.number - 1)
    case Some(tail) => tail.lastEntrySerial
  }

  private[sweeper] def enqueue(req: Request): Unit = {
    queue.put(req)
  }

  def ioThread(stream: TriFileStream): Unit = {
    var pruneId: Option[FileId] = if (stream.entry.isFull)
      Some(stream.rotateFiles())
    else
      None
    var pruned: List[Either[Tx,Alloc]] = Nil

    try {
      while (true) {

        val (commitBuffers, serial, completions) = synchronized {

          val serial = nextEntrySerialNumber

          // Move forward all data stored in the to-be-pruned file
          pruneId.foreach { fileId =>
            pruneId = None
            transactions.valuesIterator.foreach { tx =>
              val pruneTxd = tx.txdLocation match {
                case None => true
                case Some(loc) => loc.fileId == fileId
              }
              val pruneOu = tx.objectUpdateLocations.exists(ou => ou.fileId == fileId)

              if (pruneTxd || (pruneOu && tx.keepObjectUpdates)) {
                tx.txdLocation = None
                tx.objectUpdateLocations = Nil
                pruned = Left(tx) :: pruned
              }
            }
            allocations.valuesIterator.foreach { a =>
              val prune = a.dataLocation match {
                case None => true
                case Some(loc) => loc.fileId == fileId
              }

              if (prune) {
                a.dataLocation = None
                pruned = Right(a) :: pruned
              }
            }
          }

          while (!stream.entry.isFull && pruned.nonEmpty) {
            val success = pruned.head match {
              case Left(tx) => stream.entry.addTransaction(tx, contentQueue, None)
              case Right(a) => stream.entry.addAllocation(a, contentQueue, None)
            }
            if (success)
              pruned = pruned.tail
          }

          // Migrate entries behind the entry window to the front of the queue
          def migrateStaleEntries(oe: Option[LogContent]): Unit = {
            if (!stream.entry.isFull) {
              oe.foreach {
                case tx: Tx =>
                  if (tx.lastEntrySerial.number < nextEntrySerialNumber.number - maxEarliestWindow) {
                    stream.entry.addTransaction(tx, contentQueue, None)
                    migrateStaleEntries(contentQueue.tail)
                  }

                case a: Alloc =>
                  if (a.lastEntrySerial.number < nextEntrySerialNumber.number - maxEarliestWindow) {
                    stream.entry.addAllocation(a, contentQueue, None)
                    migrateStaleEntries(contentQueue.tail)
                  }
              }
            }
          }

          migrateStaleEntries(contentQueue.tail)

          nextRequest.foreach { req =>
            nextRequest = None
            handleRequest(req, stream.entry)
          }

          var wouldBlock = false

          while (!stream.entry.isFull && !wouldBlock) {
            // read until the entry is full or would block
            val req = if (stream.entry.isEmpty)
              queue.take() // block until we have something to do
            else
              queue.poll(0, TimeUnit.MICROSECONDS)

            if (req == null)
              wouldBlock = true
            else
              handleRequest(req, stream.entry)
          }

          val (fileId, fileUUID) = stream.status()

          val (buffers, completions, entryLocation) = try {
            val (buffers, completions, entryLocation) = stream.entry.commit(serial, earliestNeeded, fileUUID, fileId, lastEntryLocation)
            (buffers, completions, entryLocation)
          } catch {
            case e: Throwable => throw e
          }

          lastEntryLocation = entryLocation

          nextEntrySerialNumber = LogEntrySerialNumber(nextEntrySerialNumber.number + 1)

          (buffers, serial, completions)
        } // End synchronized block

        stream.write(commitBuffers)

        // If the entry is still full after being written, rotate the files
        if (stream.entry.isFull) {
          pruneId = Some(stream.rotateFiles())
        }

        synchronized {

          pendingNotifications += (serial -> completions)

          def notify(c: SaveCompletion): Unit = clients.get(c.clientId).foreach(h => h.saveComplete(c))

          def notifyInOrder(ser: LogEntrySerialNumber): Unit = {
            pendingNotifications.get(ser) match {
              case None =>
              case Some(l) =>
                l.foreach(notify)
                pendingNotifications -= ser
                lastNotified = ser
                notifyInOrder(LogEntrySerialNumber(lastNotified.number + 1))
            }
          }

          notifyInOrder(LogEntrySerialNumber(lastNotified.number + 1))
        }
      }
    } catch {
      case ExitThread => // Exit requested
    }
  }

  private def handleRequest(req: Request, entry:Entry): Unit = req match {
    case r: TxSave =>
      val txid = TxId(r.state.storeId, r.transactionId)
      val tx = transactions.get(txid) match {
        case None =>
          new Tx(txid, r.state, nextEntrySerialNumber, None, Nil, true)

        case Some(t) =>
          t.state = r.state
          t
      }
      if (entry.addTransaction(tx, contentQueue, Some(r.client, r.saveId)))
        transactions += (txid -> tx)
      else
        nextRequest = Some(req)

    case a: AllocSave =>
      val txid = TxId(a.state.storeId, a.state.allocationTransactionId)
      val alloc = new Alloc(None, a.state, nextEntrySerialNumber)
      if (entry.addAllocation(alloc, contentQueue, Some(a.client, txid)))
        allocations += (txid -> alloc)
      else
        nextRequest = Some(req)

    case d: DropTxData =>
      transactions.get(d.txid).foreach { tx =>
        tx.keepObjectUpdates = false
        tx.objectUpdateLocations = Nil
      }

    case d: DeleteTx =>
      if (entry.addDeleteTransaction(d.txid))
        transactions -= d.txid
      else
        nextRequest = Some(req)

    case d: DeleteAlloc =>
      if (entry.addDeleteAllocation(d.txid))
        allocations -= d.txid
      else
        nextRequest = Some(req)

    case f: GetFullStoreState =>
      val t = transactions.valuesIterator.filter(tx => tx.state.storeId == f.storeId).map(tx => tx.state).toList
      val a = allocations.valuesIterator.filter(a => a.state.storeId == f.storeId).map(a => a.state).toList
      f.response.put((t,a))

    case _: ExitIOThread => throw ExitThread

  }

  def getFullRecoveryState(storeId: StoreId): (List[TransactionRecoveryState], List[AllocationRecoveryState]) = {
    val responder = new LinkedBlockingQueue[(List[TransactionRecoveryState], List[AllocationRecoveryState])]

    enqueue(GetFullStoreState(storeId, responder))

    responder.take()
  }

  def createCRL(completionHandler: SaveCompletionHandler): CrashRecoveryLog = synchronized {
    val id = CrashRecoveryLogClient(nextClient)
    nextClient += 1
    clients += ( id -> completionHandler)
    new SweeperCRL(this, id)
  }

  def getData(loc: FileLocation): DataBuffer = {
    if (loc.fileId.number >= files.length)
      throw new CorruptedEntry(s"Invalid File Index ${loc.fileId.number}")
    files(loc.fileId.number).read(loc.offset, loc.length)
  }

  def recover(): RecoveringState = {
    val rfiles = files.sortBy(f => f.findLastValidEntry().
      getOrElse((LogEntrySerialNumber(0), 0))._1.number).reverse.toList

    for (f <- rfiles) {
      try {
        return loadState(f)
      } catch {
        case e: CorruptedEntry => logger.warn(s"Corrupted CRL Entry: ${e.getMessage}")
      }
    }
    new RecoveringState
  }

  private def loadState(lastFile: LogFile): RecoveringState = {
    val rs = new RecoveringState

    lastFile.findLastValidEntry() match {
      case None =>
        // No state to load. We're done

      case Some((lastSerial, offset)) =>
        loadEntry(lastFile.fileId, offset, rs, None, lastSerial)
    }

    rs
  }

  private def loadEntry(file: FileId,
                        offset: Long,
                        state: RecoveringState,
                        earliestSerial: Option[Long],
                        lastSerial: LogEntrySerialNumber): Unit = {

    val footer = files(file.number).read(offset, Entry.StaticEntryFooterSize)
    val serial = footer.getLong()
    val entryOffset = footer.getLong()
    val thisEarliest = footer.getLong()
    val numTx = footer.getInt()
    val numAlloc = footer.getInt()
    val numTxDeletions = footer.getInt()
    val numAllocDeletions = footer.getInt()
    val prevEntryLocation = getFileLocation(footer)

    val entry = files(file.number).read(entryOffset, (offset - entryOffset).asInstanceOf[Int])

    if (state.lastEntry.isEmpty)
      state.lastEntry = Some((lastSerial, FileLocation(file, offset, Entry.StaticEntryFooterSize)))

    for (_ <- 0 until numTx) {
      val txid = getTxId(entry)
      val keep = !state.transactions.contains(txid) && !state.deletedTx.contains(txid)
      val txdLoc = getFileLocation(entry)
      val disposition = entry.get() match {
        case 0 => TransactionDisposition.Undetermined
        case 1 => TransactionDisposition.VoteCommit
        case 2 => TransactionDisposition.VoteAbort
        case _ => throw new CorruptedEntry("Invalid Transaction Disposition")
      }
      val pmask = entry.get()
      val prom_num = entry.getInt()
      val prom_peer = entry.get()
      val accept_num = entry.getInt()
      val accept_peer = entry.get()
      val promised = if ((pmask & 1 << 0) != 0) Some(ProposalId(prom_num, prom_peer)) else None
      val accepted = if ((pmask & 1 << 1) != 0) Some((ProposalId(accept_num, accept_peer), (pmask & 1 << 2) != 0)) else None
      val paxState = PersistentState(promised, accepted)
      val numObjectUpdates = entry.getInt()
      var objectUpdates: List[ObjectUpdate] = Nil
      var updateLocations: List[FileLocation] = Nil
      for (_ <- 0 until numObjectUpdates) {
        val uuid = getUUID(entry)
        val loc = getFileLocation(entry)
        if (keep) {
          val data = DataBuffer(files(loc.fileId.number).read(loc.offset, loc.length))
          objectUpdates = ObjectUpdate(uuid, data) :: objectUpdates
          updateLocations = loc :: updateLocations
        }
      }
      if (keep) {
        val serializedTxd = files(txdLoc.fileId.number).read(txdLoc.offset, txdLoc.length)
        val trs = TransactionRecoveryState(txid.storeId, serializedTxd, objectUpdates, disposition,
          TransactionStatus.Unresolved,paxState)

        val tx = new Tx(txid, trs, LogEntrySerialNumber(serial), Some(txdLoc), updateLocations, keepObjectUpdates = true)

        state.transactions += (txid -> tx)
      }
    }

    for (_ <- 0 until numAlloc) {
      val txid = getTxId(entry)
      val keep = !state.allocations.contains(txid) && !state.deletedAlloc.contains(txid)
      val sparr = new Array[Byte](entry.getInt())
      entry.get(sparr)
      val transactionId = TransactionId(getUUID(entry))
      val storePointer = StorePointer(txid.storeId.poolIndex, sparr)
      val id = ObjectId(getUUID(entry))
      val objectType = entry.get() match {
        case 0 => ObjectType.Data
        case 1 => ObjectType.KeyValue
        case _ => throw new CorruptedEntry("Invalid Object Type")
      }
      val sz = entry.getInt()
      val size = if (sz != 0) Some(sz) else None
      val dataLoc = getFileLocation(entry)
      val rserial = entry.getInt()
      val rcount = entry.getInt()
      val refcount = ObjectRefcount(rserial, rcount)
      val timestamp = HLCTimestamp(entry.getLong())
      val serializedRevisionGuard = new Array[Byte](entry.getInt())
      entry.get(serializedRevisionGuard)

      if (keep) {
        val data = files(dataLoc.fileId.number).read(dataLoc.offset, dataLoc.length)
        val ars = AllocationRecoveryState(txid.storeId, storePointer, id, objectType, size,
          DataBuffer(data), refcount, timestamp, transactionId, serializedRevisionGuard)

        val alloc = new Alloc(Some(dataLoc), ars, LogEntrySerialNumber(serial))

        state.allocations += (txid -> alloc)
      }
    }

    for (_ <- 0 until numTxDeletions) {
      state.deletedTx += getTxId(entry)
    }

    for (_ <- 0 until numAllocDeletions) {
      state.deletedAlloc += getTxId(entry)
    }

    val earliest = earliestSerial.getOrElse(thisEarliest)

    if (serial != earliest && serial != 0)
      loadEntry(prevEntryLocation.fileId, prevEntryLocation.offset, state, Some(earliest), state.lastEntry.get._1)
  }
}
