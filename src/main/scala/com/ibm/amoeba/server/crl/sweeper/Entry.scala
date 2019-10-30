package com.ibm.amoeba.server.crl.sweeper

import scala.collection.immutable.HashMap

class Entry(val max_size: Int) {
  import Entry._

  var requests: HashMap[TxId, Completion] = new HashMap()
  var txs: HashMap[TxId, Tx] = new HashMap()
  var txDeletions: List[TxId] = Nil
  var allocations: List[Alloc] = Nil
  var allocDeletions: List[TxId] = Nil
  var size: Long = 0
  var offset: Long = 0

  // Track total size of the Entry and check before each addition for exceeding
  // maximum size or operations
  //def addTransaction(txid: TxId, tx: Tx, clientRequest: ClientRequest): Boolean
}

object Entry {
  /// store::Id + UUID
  val TxidSize: Int = 17 + 16

  // 2 byte file id + 8 byte offset + 4 byte length
  val FileLocationSize: Int = 2 + 8 + 4

  // Static Transaction Block Size {
  //     store_id:  17 (16-bye pool id + 1 byte index)
  //     transaction_id: 16
  //     serialized_transaction_description: Bytes, 14 (FileLocation)
  //     tx_disposition: transaction::Disposition, 1
  //     paxos_state: paxos::PersistentState, 11 (1:mask-byte + 5:proposalId + 5:proposalId)
  //     object_updates: Vec<transaction::ObjectUpdate>, 4:count (trailing data is num_updates * (16:objuuid + FileLocation))
  // }
  val StaticTxSize: Int = TxidSize + FileLocationSize + 1 + 11 + 4

  // Update format is 16-byte object UUID + FileLocation
  val ObjectUpdateStaticSize: Int = 16 + FileLocationSize

  /// Entry block always ends with:
  ///   entry_serial_number - 8
  ///   entry_begin_offset - 8
  ///   earliest_entry_needed - 8
  ///   num_transactions - 4
  ///   num_allocations - 4
  ///   num_tx_deletions - 4
  ///   num_alloc_deletions - 4
  ///   prev_entry_file_location - 14 (2 + 8 + 4)
  ///   file_uuid - 16
  val StaticEntrySize: Int = 8 + 8 + 8 + 4 + 4 + 4 + 4 + 14 + 16

  // pub struct AllocationRecoveryState {
  //     store_id: store::Id, 17
  //     allocation_transaction_id: transaction::Id, 16
  //     store_pointer: object::StorePointer,  <== 4 + nbytes
  //     id: object::Id, 16
  //     kind: object::Kind, 1
  //     size: Option<u32>, 4 - 0 means None
  //     data: Bytes, 14 = FileLocation
  //     refcount: object::Refcount, 8
  //     timestamp: hlc::Timestamp, 8
  //     serialized_revision_guard: Bytes <== 4 + nbytes
  // }
  val StaticArsSize: Int = 17 + 16 + 4 + 16 + 1 + 4 + 14 + 8 + 8 + 4
}
