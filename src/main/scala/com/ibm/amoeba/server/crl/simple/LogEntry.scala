package com.ibm.amoeba.server.crl.simple

import scala.collection.immutable.HashMap

class LogEntry(val maxSize: Long, initialFileSize: Long) {
  import LogEntry._

  private var completionHandlers: List[() => Unit] = Nil
  var txs: HashMap[TxId, Tx] = new HashMap()
  var txDeletions: List[TxId] = Nil
  var allocations: List[Alloc] = Nil
  var allocDeletions: List[TxId] = Nil
  var staticSize: Int = 0
  var dataSize: Long = 0
  var offset: Long = 0

}

object LogEntry:
  /// Entry Header
  ///   entry_serial_number - 8
  ///   entry_begin_offset - 8
  ///   earliest_entry_needed - 8
  ///   num_transactions - 4
  ///   num_allocations - 4
  ///   num_tx_deletions - 4
  ///   num_alloc_deletions - 4
  ///   prev_entry_file_location - 14 (2 + 8 + 4)
  ///   file_uuid - 16
  val StaticEntryHeaderSize: Int = 8 + 8 + 8 + 4 + 4 + 4 + 4 + 14 + 16