package com.ibm.amoeba.server.crl

import java.util.UUID

import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.objects.{ObjectRefcount, ObjectType}
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.TransactionId

case class AllocationRecoveryState(
                                    storeId: StoreId,
                                    storePointer: StorePointer,
                                    newObjectUUID: UUID,
                                    objectType: ObjectType.Value,
                                    objectSize: Option[Int],
                                    objectData: DataBuffer,
                                    initialRefcount: ObjectRefcount,
                                    timestamp: HLCTimestamp,
                                    allocationTransactionId: TransactionId,
                                    serializedRevisionGuard: DataBuffer
                                  )
