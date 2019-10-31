package com.ibm.amoeba.common.network

import java.util.UUID

import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.objects.{AllocationRevisionGuard, ObjectRefcount, ObjectType}
import com.ibm.amoeba.common.paxos.ProposalId
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.{TransactionDescription, TransactionDisposition, TransactionStatus}

sealed abstract class Message

sealed abstract class ClientMessage extends Message

sealed abstract class TxMessage extends Message {
  val to: StoreId
  val from: StoreId
}

final case class Allocate(
                           toStore: StoreId,
                           fromClient: ClientId,
                           newObjectUUID: UUID,
                           objectType: ObjectType.Value,
                           objectSize: Option[Int],
                           initialRefcount: ObjectRefcount,
                           objectData: DataBuffer,
                           timestamp: HLCTimestamp,
                           allocationTransactionUUID: UUID,
                           revisionGuard: AllocationRevisionGuard
                         ) extends ClientMessage {

  override def equals(other: Any): Boolean = other match {
    case rhs: Allocate => toStore == rhs.toStore && fromClient == rhs.fromClient &&
      objectSize == rhs.objectSize && objectData.compareTo(rhs.objectData) == 0 &&
      initialRefcount == rhs.initialRefcount && timestamp.compareTo(rhs.timestamp) == 0 &&
      allocationTransactionUUID == rhs.allocationTransactionUUID &&
      revisionGuard == rhs.revisionGuard
    case _ => false
  }
}

final case class AllocateResponse(
                                   fromStoreId: StoreId,
                                   allocationTransactionUUID: UUID,
                                   newObjectUUID: UUID,
                                   result: Option[StorePointer]) extends ClientMessage


final case class TxPrepare(
                            to: StoreId,
                            from: StoreId,
                            txd: TransactionDescription,
                            proposalId: ProposalId) extends TxMessage

final case class TxPrepareResponse(
                                    to: StoreId,
                                    from: StoreId,
                                    transactionUUID: UUID,
                                    response: Either[TxPrepareResponse.Nack, TxPrepareResponse.Promise],
                                    proposalId: ProposalId,
                                    disposition: TransactionDisposition.Value) extends TxMessage

object TxPrepareResponse {
  case class Nack(promisedId: ProposalId)
  case class Promise(lastAccepted: Option[(ProposalId,Boolean)])
}

final case class TxAccept(
                           to: StoreId,
                           from: StoreId,
                           transactionUUID: UUID,
                           proposalId: ProposalId,
                           value: Boolean) extends TxMessage

final case class TxAcceptResponse(
                                   to: StoreId,
                                   from: StoreId,
                                   transactionUUID: UUID,
                                   proposalId: ProposalId,
                                   response: Either[TxAcceptResponse.Nack, TxAcceptResponse.Accepted]) extends TxMessage

object TxAcceptResponse {
  case class Nack(promisedId: ProposalId)
  case class Accepted(value: Boolean)
}

final case class TxResolved(
                             to: StoreId,
                             from: StoreId,
                             transactionUUID: UUID,
                             committed: Boolean) extends TxMessage

final case class TxCommitted(
                              to: StoreId,
                              from: StoreId,
                              transactionUUID: UUID,
                              // List of object UUIDs that could not be committed due to transaction requirement errors
                              objectCommitErrors: List[UUID]) extends TxMessage

final case class TxFinalized(
                              to: StoreId,
                              from: StoreId,
                              transactionUUID: UUID,
                              committed: Boolean) extends TxMessage

final case class TxHeartbeat(
                              to: StoreId,
                              from: StoreId,
                              transactionUUID: UUID) extends TxMessage

final case class TxStatusRequest(
                                  to: StoreId,
                                  from: StoreId,
                                  transactionUUID: UUID,
                                  requestUUID: UUID) extends TxMessage

final case class TxStatusResponse(
                                   to: StoreId,
                                   from: StoreId,
                                   transactionUUID: UUID,
                                   requestUUID: UUID,
                                   status: Option[TxStatusResponse.TxStatus]) extends TxMessage

object TxStatusResponse {
  case class TxStatus(status: TransactionStatus.Value, finalized: Boolean)
}

