package com.ibm.amoeba.server.cnc

import com.ibm.amoeba.client.HostId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.server.store.backend.BackendType


sealed abstract class CnCRequest

case class NewStore(storeId: StoreId, backendType: BackendType) extends CnCRequest

case class ShutdownStore(storeId: StoreId) extends CnCRequest

case class TransferStore(storeId: StoreId, toHost: HostId) extends CnCRequest

