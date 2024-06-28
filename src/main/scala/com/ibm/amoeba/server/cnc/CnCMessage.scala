package com.ibm.amoeba.server.cnc

import com.ibm.amoeba.client.HostId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.server.store.backend.BackendType


sealed abstract class CnCMessage

case class NewStore(storeId: StoreId, backendType: BackendType) extends CnCMessage

case class ShutdownStore(storeId: StoreId) extends CnCMessage

case class TransferStore(storeId: StoreId, toHost: HostId) extends CnCMessage
