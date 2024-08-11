package org.aspen_ddp.aspen.server.cnc

import org.aspen_ddp.aspen.client.HostId
import org.aspen_ddp.aspen.common.store.StoreId
import org.aspen_ddp.aspen.server.store.backend.BackendType


sealed abstract class CnCRequest

case class NewStore(storeId: StoreId, backendType: BackendType) extends CnCRequest

case class ShutdownStore(storeId: StoreId) extends CnCRequest

case class TransferStore(storeId: StoreId, toHost: HostId) extends CnCRequest

