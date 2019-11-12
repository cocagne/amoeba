package com.ibm.amoeba.common.paxos

class Acceptor(val peerId: Byte,
               initialState: PersistentState = PersistentState(None, None)) {

  private[this] var state = initialState

  def persistentState: PersistentState = state

  def receivePrepare(m: Prepare): Either[Nack,Promise] = {
    def prepare = {
      state = state.copy(promised = Some(m.proposalId))
      Right(Promise(peerId, m.proposalId, state.accepted))
    }

    state.promised match {
      case Some(promisedId) =>
        if (m.proposalId >= promisedId)
          prepare
        else
          Left(Nack(peerId, m.proposalId, promisedId))

      case None => prepare
    }
  }

  def receiveAccept(m: Accept): Either[Nack,Accepted] = {
    def accept = {
      state = state.copy(promised = Some(m.proposalId), accepted=Some((m.proposalId, m.proposalValue)))
      Right(Accepted(peerId, m.proposalId, m.proposalValue))
    }

    state.promised match {
      case Some(promisedId) =>
        if (m.proposalId >= promisedId)
          accept
        else
          Left(Nack(peerId, m.proposalId, promisedId))

      case None => accept
    }
  }
}