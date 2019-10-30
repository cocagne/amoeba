package com.ibm.amoeba.common.network

import java.util.UUID

import com.ibm.amoeba.common.paxos.ProposalId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.{TransactionDescription, TransactionDisposition, TransactionStatus}

sealed abstract class Message {
  val to: StoreId
  val from: StoreId
}

final case class TxPrepare(
                            to: StoreId,
                            from: StoreId,
                            txd: TransactionDescription,
                            proposalId: ProposalId) extends Message

final case class TxPrepareResponse(
                                    to: StoreId,
                                    from: StoreId,
                                    transactionUUID: UUID,
                                    response: Either[TxPrepareResponse.Nack, TxPrepareResponse.Promise],
                                    proposalId: ProposalId,
                                    disposition: TransactionDisposition.Value) extends Message

object TxPrepareResponse {
  case class Nack(promisedId: ProposalId)
  case class Promise(lastAccepted: Option[(ProposalId,Boolean)])
}

final case class TxAccept(
                           to: StoreId,
                           from: StoreId,
                           transactionUUID: UUID,
                           proposalId: ProposalId,
                           value: Boolean) extends Message

final case class TxAcceptResponse(
                                   to: StoreId,
                                   from: StoreId,
                                   transactionUUID: UUID,
                                   proposalId: ProposalId,
                                   response: Either[TxAcceptResponse.Nack, TxAcceptResponse.Accepted]) extends Message

object TxAcceptResponse {
  case class Nack(promisedId: ProposalId)
  case class Accepted(value: Boolean)
}

final case class TxResolved(
                             to: StoreId,
                             from: StoreId,
                             transactionUUID: UUID,
                             committed: Boolean) extends Message

final case class TxCommitted(
                              to: StoreId,
                              from: StoreId,
                              transactionUUID: UUID,
                              // List of object UUIDs that could not be committed due to transaction requirement errors
                              objectCommitErrors: List[UUID]) extends Message

final case class TxFinalized(
                              to: StoreId,
                              from: StoreId,
                              transactionUUID: UUID,
                              committed: Boolean) extends Message

final case class TxHeartbeat(
                              to: StoreId,
                              from: StoreId,
                              transactionUUID: UUID) extends Message

final case class TxStatusRequest(
                                  to: StoreId,
                                  from: StoreId,
                                  transactionUUID: UUID,
                                  requestUUID: UUID) extends Message

final case class TxStatusResponse(
                                   to: StoreId,
                                   from: StoreId,
                                   transactionUUID: UUID,
                                   requestUUID: UUID,
                                   status: Option[TxStatusResponse.TxStatus]) extends Message

object TxStatusResponse {
  case class TxStatus(status: TransactionStatus.Value, finalized: Boolean)
}

