package com.ibm.amoeba.server.crl

import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.objects.{ObjectId, ObjectRefcount, ObjectType}
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.TransactionId

case class AllocationRecoveryState(
                                    storeId: StoreId,
                                    storePointer: StorePointer,
                                    newObjectId: ObjectId,
                                    objectType: ObjectType.Value,
                                    objectSize: Option[Int],
                                    objectData: DataBuffer,
                                    initialRefcount: ObjectRefcount,
                                    timestamp: HLCTimestamp,
                                    allocationTransactionId: TransactionId,
                                    serializedRevisionGuard: DataBuffer
                                  )
