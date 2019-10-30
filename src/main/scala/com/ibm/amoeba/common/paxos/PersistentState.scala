package com.ibm.amoeba.common.paxos

case class PersistentState(
                            promised: Option[ProposalId],
                            accepted: Option[(ProposalId, Boolean)])
