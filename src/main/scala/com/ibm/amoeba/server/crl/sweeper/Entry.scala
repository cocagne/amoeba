package com.ibm.amoeba.server.crl.sweeper

import java.nio.ByteBuffer
import java.util.UUID

import com.ibm.amoeba.common.objects.ObjectType
import com.ibm.amoeba.common.transaction.TransactionDisposition
import com.ibm.amoeba.server.crl.{AllocSaveComplete, CrashRecoveryLogClient, SaveCompletion, TxSaveComplete, TxSaveId}

import scala.collection.immutable.HashMap

class Entry(val maxSize: Long, initialFileSize: Long) {
  import Entry._

  private var completions: List[SaveCompletion] = Nil
  var txs: HashMap[TxId, Tx] = new HashMap()
  var txDeletions: List[TxId] = Nil
  var allocations: List[Alloc] = Nil
  var allocDeletions: List[TxId] = Nil
  var staticSize: Int = 0
  var dataSize: Long = 0
  var offset: Long = initialFileSize

  private var full: Boolean = !haveRoomFor(SubEntry(0, 4096))

  def isFull: Boolean = {
    full
  }

  private[sweeper] def rotated(): Unit = {
    full = false
    offset = 16
  }

  def isEmpty: Boolean = {
    txs.isEmpty && allocations.isEmpty && txDeletions.isEmpty && allocDeletions.isEmpty
  }

  private def haveRoomFor(subEntry: SubEntry): Boolean = {
    offset + dataSize + staticSize + subEntry.totalSize + 4096 <= maxSize
  }

  def commit(serial: LogEntrySerialNumber,
             earliestNeeded: LogEntrySerialNumber,
             fileUUID: UUID,
             fileId: FileId,
             previousEntryFooterLocation: FileLocation): (Array[ByteBuffer], List[SaveCompletion], FileLocation) = {

    val padding = padTo4kAlignment(offset, dataSize, staticSize + StaticEntryFooterSize)

    val entryBuffer = ByteBuffer.allocate(staticSize + padding + StaticEntryFooterSize)
    val entryOffset = offset + dataSize
    val footerOffset = entryOffset + staticSize + padding

    //println(s"offset $offset entry offset $entryOffset staticSize $staticSize padding $padding footer $footerOffset. Footer size $StaticEntryFooterSize")

    var buffers: List[ByteBuffer] = Nil
    var off = offset

    def putUUID(uuid: UUID): Unit = {
      entryBuffer.putLong(uuid.getMostSignificantBits)
      entryBuffer.putLong(uuid.getLeastSignificantBits)
    }
    def putTxId(txid: TxId): Unit = {
      putUUID(txid.storeId.poolId.uuid)
      entryBuffer.put(txid.storeId.poolIndex)
      putUUID(txid.transactionId.uuid)
    }
    def putFileLocation(loc: FileLocation): Unit = {
      entryBuffer.putShort(loc.fileId.number.asInstanceOf[Short])
      entryBuffer.putLong(loc.offset)
      entryBuffer.putInt(loc.length)
    }

    // Transaction Entry
    //     store_id:  17 (16-bye pool id + 1 byte index)
    //     transaction_id: 16
    //     serialized_transaction_description: FileLocation, 14
    //     tx_disposition: TransactionDisposition, 1
    //     paxos_state: paxos::PersistentState, 11 (1:mask-byte + 5:proposalId + 5:proposalId)
    //         bit 0 - have promise proposal
    //         bit 1 - have accepted proposal
    //         bit 2 - accepted boolean value (only valid if bit 1 is set)
    //     object_updates: 4-byte-count, num_updates * (16:objuuid + FileLocation))
    //
    txs.valuesIterator.foreach { tx =>
      tx.lastEntrySerial = serial

      putTxId(tx.id)

      val txdLoc: FileLocation = tx.txdLocation match {
        case Some(loc) => loc
        case None =>
          val sz = tx.state.serializedTxd.size
          val loc = FileLocation(fileId, off, sz)
          tx.txdLocation = Some(loc)
          buffers = tx.state.serializedTxd.asReadOnlyBuffer() :: buffers
          off += sz
          loc
      }
      putFileLocation(txdLoc)

      val encodedDisposition = tx.state.disposition match {
        case TransactionDisposition.Undetermined => 0
        case TransactionDisposition.VoteCommit => 1
        case TransactionDisposition.VoteAbort => 2
      }

      entryBuffer.put(encodedDisposition.asInstanceOf[Byte])

      var mask = 0
      if (tx.state.paxosAcceptorState.promised.isDefined)
        mask |= 1 << 0
      tx.state.paxosAcceptorState.accepted.foreach { t =>
        mask |= 1 << 1
        if (t._2) {
          mask |= 1 << 2 // Mark as accepted
        }
      }

      entryBuffer.put(mask.asInstanceOf[Byte])

      tx.state.paxosAcceptorState.promised match {
        case None =>
          entryBuffer.putInt(0)
          entryBuffer.put(0.asInstanceOf[Byte])
        case Some(p) =>
          entryBuffer.putInt(p.number)
          entryBuffer.put(p.peer)
      }
      tx.state.paxosAcceptorState.accepted match {
        case None =>
          entryBuffer.putInt(0)
          entryBuffer.put(0.asInstanceOf[Byte])
        case Some(t) =>
          entryBuffer.putInt(t._1.number)
          entryBuffer.put(t._1.peer)
      }

      if (tx.keepObjectUpdates && tx.objectUpdateLocations.isEmpty) {
        for (ou <- tx.state.objectUpdates) {
          val sz = ou.data.size
          val loc = FileLocation(fileId, off, sz)
          tx.objectUpdateLocations = loc :: tx.objectUpdateLocations
          buffers = ou.data.asReadOnlyBuffer() :: buffers
          off += sz
        }
        // reverse list they match positionally for the zip operation below
        tx.objectUpdateLocations = tx.objectUpdateLocations.reverse
      }

      val numObjectUpdates = if (tx.keepObjectUpdates) tx.state.objectUpdates.length else 0

      entryBuffer.putInt(numObjectUpdates)

      if (tx.keepObjectUpdates) {
        tx.objectUpdateLocations.zip(tx.state.objectUpdates).foreach { t =>
          putUUID(t._2.objectId.uuid)
          putFileLocation(t._1)
        }
      }
    }

    // Allocation Entry
    //     store_id: StoreId, 17
    //     allocation_transaction_id: TransactionId, 16
    //     store_pointer: StorePointer, 4 + nbytes
    //     id: ObjectId, 16
    //     objectType: ObjectType, 1
    //     size: Option[Int], 4 - 0 means None
    //     data: FileLocation, 14
    //     refcount: Refcount, 8 (4-byte serial, 4-byte count)
    //     timestamp: HLCTimestamp, 8
    //     serialized_revision_guard: DataBuffer <== 4 + nbytes
    //

    allocations.foreach { alloc =>
      alloc.lastEntrySerial = serial

      val dataLoc: FileLocation = alloc.dataLocation match {
        case Some(loc) => loc
        case None =>
          val sz = alloc.state.objectData.size
          val loc = FileLocation(fileId, off, sz)
          alloc.dataLocation = Some(loc)
          buffers = alloc.state.objectData.asReadOnlyBuffer() :: buffers
          off += sz
          loc
      }

      putTxId(TxId(alloc.state.storeId, alloc.state.allocationTransactionId))
      entryBuffer.putInt(alloc.state.storePointer.data.length)
      entryBuffer.put(alloc.state.storePointer.data)
      putUUID(alloc.state.newObjectId.uuid)
      val kind = alloc.state.objectType match {
        case ObjectType.Data => 0
        case ObjectType.KeyValue => 1
      }
      entryBuffer.put(kind.asInstanceOf[Byte])
      entryBuffer.putInt(alloc.state.objectSize.getOrElse(0))
      putFileLocation(dataLoc)
      entryBuffer.putInt(alloc.state.initialRefcount.updateSerial)
      entryBuffer.putInt(alloc.state.initialRefcount.count)
      entryBuffer.putLong(alloc.state.timestamp.asLong)
      entryBuffer.putInt(alloc.state.serializedRevisionGuard.size)
      entryBuffer.put(alloc.state.serializedRevisionGuard.asReadOnlyBuffer())
    }

    txDeletions.foreach { txid => putTxId(txid) }
    allocDeletions.foreach { txid => putTxId(txid) }


    (1 to padding).foreach { _ => entryBuffer.put(0.asInstanceOf[Byte]) }

    /// Entry Footer
    ///   entry_serial_number - 8
    ///   entry_begin_offset - 8
    ///   earliest_entry_needed - 8
    ///   num_transactions - 4
    ///   num_allocations - 4
    ///   num_tx_deletions - 4
    ///   num_alloc_deletions - 4
    ///   prev_entry_file_location - 14 (2 + 8 + 4)
    ///   file_uuid - 16
    entryBuffer.putLong(serial.number)
    entryBuffer.putLong(entryOffset)
    entryBuffer.putLong(earliestNeeded.number)
    entryBuffer.putInt(txs.size)
    entryBuffer.putInt(allocations.size)
    entryBuffer.putInt(txDeletions.size)
    entryBuffer.putInt(allocDeletions.size)
    putFileLocation(previousEntryFooterLocation)
    putUUID(fileUUID)

    //println(s"Write serial $serial ntx ${txs.size} nall ${allocations.size} ntxd ${txDeletions.size} ad ${allocDeletions.size}")
    entryBuffer.position(0)

    buffers = entryBuffer :: buffers

    offset += buffers.foldLeft(0L){ (sz, b) => sz + b.remaining() }

    txs = txs.empty
    allocations = Nil
    txDeletions = Nil
    allocDeletions = Nil
    staticSize = 0
    dataSize = 0

    val clist = completions
    completions = Nil

    val entryLocation = FileLocation(fileId, footerOffset, StaticEntryFooterSize)

    //println(s"Entry location for $serial: $entryLocation. Previous loc: $previousEntryFooterLocation")
    (buffers.reverse.toArray, clist, entryLocation)
  }

  def addTransaction(tx: Tx,
                     contentQueue: LogContentQueue,
                     req: Option[(CrashRecoveryLogClient, TxSaveId)]): Boolean = {

    if (txs.contains(tx.id)) {
      req.foreach { t =>
        completions = TxSaveComplete(t._1, tx.id.storeId, tx.id.transactionId, t._2) :: completions
      }
      true
    } else {
      val sub = txWriteSize(tx)
      if (haveRoomFor(sub)) {
        dataSize += sub.dataSize
        staticSize += sub.staticSize
        txs += (tx.id -> tx)
        contentQueue.moveToHead(tx)
        req.foreach { t =>
          completions = TxSaveComplete(t._1, tx.id.storeId, tx.id.transactionId, t._2) :: completions
        }
        true
      } else {
        full = true
        false
      }
    }
  }

  def addAllocation(alloc: Alloc,
                    contentQueue: LogContentQueue,
                    req: Option[(CrashRecoveryLogClient, TxId)]): Boolean = {

    val sub = allocWriteSize(alloc)
    if (haveRoomFor(sub)) {
      dataSize += sub.dataSize
      staticSize += sub.staticSize
      allocations = alloc :: allocations
      contentQueue.moveToHead(alloc)
      req.foreach { t =>
        val txid = t._2
        completions = AllocSaveComplete(t._1, txid.transactionId, txid.storeId, alloc.state.newObjectId) :: completions
      }
      true
    } else {
      full = true
      false
    }
  }

  def addDeleteTransaction(txid: TxId): Boolean = {
    val sub = txDeleteSize()
    if (haveRoomFor(sub)) {
      dataSize += sub.dataSize
      staticSize += sub.staticSize
      txDeletions = txid :: txDeletions
      true
    } else {
      full = true
      false
    }
  }

  def addDeleteAllocation(txid: TxId): Boolean = {
    val sub = txDeleteSize()
    if (haveRoomFor(sub)) {
      dataSize += sub.dataSize
      staticSize += sub.staticSize
      allocDeletions = txid :: allocDeletions
      true
    } else {
      full = true
      false
    }
  }
}

object Entry {

  /// store::Id + UUID
  val TxidSize: Int = 17 + 16

  // 2 byte file id + 8 byte offset + 4 byte length
  val FileLocationSize: Int = 2 + 8 + 4

  // Transaction Entry
  //     store_id:  17 (16-bye pool id + 1 byte index)
  //     transaction_id: 16
  //     serialized_transaction_description: FileLocation, 14
  //     tx_disposition: TransactionDisposition, 1
  //     paxos_state: paxos::PersistentState, 11 (1:mask-byte + 5:proposalId + 5:proposalId)
  //         bit 0 - have promise proposal
  //         bit 1 - have accepted proposal
  //         bit 2 - accepted boolean value (only valid if bit 1 is set)
  //     object_updates: 4-byte-count, num_updates * (16:objuuid + FileLocation))
  //
  val StaticTxSize: Int = TxidSize + FileLocationSize + 1 + 11 + 4

  // Update format is 16-byte object UUID + FileLocation
  val ObjectUpdateStaticSize: Int = 16 + FileLocationSize

  /// Entry Footer
  ///   entry_serial_number - 8
  ///   entry_begin_offset - 8
  ///   earliest_entry_needed - 8
  ///   num_transactions - 4
  ///   num_allocations - 4
  ///   num_tx_deletions - 4
  ///   num_alloc_deletions - 4
  ///   prev_entry_file_location - 14 (2 + 8 + 4)
  ///   file_uuid - 16
  val StaticEntryFooterSize: Int = 8 + 8 + 8 + 4 + 4 + 4 + 4 + 14 + 16

  // Allocation Entry
  //     store_id: StoreId, 17
  //     allocation_transaction_id: TransactionId, 16
  //     store_pointer: StorePointer, 4 + nbytes
  //     id: ObjectId, 16
  //     objectType: ObjectType, 1
  //     size: Option[Int], 4 - 0 means None
  //     data: FileLocation, 14
  //     refcount: Refcount, 8 (4-byte serial, 4-byte count)
  //     timestamp: HLCTimestamp, 8
  //     serialized_revision_guard: DataBuffer <== 4 + nbytes
  //
  val StaticArsSize: Int = 17 + 16 + 4 + 16 + 1 + 4 + 14 + 8 + 8 + 4

  case class SubEntry(dataSize: Long, staticSize: Int) {
    def totalSize: Long = dataSize + staticSize
  }

  def txWriteSize(tx: Tx): SubEntry = {
    var data = 0
    val updateCount = if (tx.keepObjectUpdates) tx.state.objectUpdates.length else 0

    if (tx.txdLocation.isEmpty)
      data += tx.state.serializedTxd.size

    if (tx.keepObjectUpdates && tx.objectUpdateLocations.isEmpty) {
      tx.state.objectUpdates.foreach { ou =>
        data += ou.data.size
      }
    }

    SubEntry(data, StaticTxSize + (updateCount * ObjectUpdateStaticSize))
  }

  def txDeleteSize(): SubEntry = SubEntry(0, TxidSize)

  def allocWriteSize(alloc: Alloc): SubEntry = {
    var data = 0
    var stat = 0

    if (alloc.dataLocation.isEmpty)
      data += alloc.state.objectData.size

    stat += alloc.state.storePointer.data.length
    stat += alloc.state.serializedRevisionGuard.size

    SubEntry(data, StaticArsSize + stat)
  }

  def allocDeleteSize(): SubEntry = SubEntry(0, TxidSize)

  def padTo4kAlignment(offset: Long, dataSize: Long, staticSize: Long): Int = {
    val base = offset + dataSize + staticSize
    if (base < 4096) {
      4096 - base.asInstanceOf[Int]
    } else {
      val remainder = base % 4096
      if (remainder == 0) {
        0
      } else {
        4096 - remainder.asInstanceOf[Int]
      }
    }
  }
}
