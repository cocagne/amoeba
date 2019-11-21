package com.ibm.amoeba.common.transaction

import com.ibm.amoeba.common.DataBuffer
import com.ibm.amoeba.common.objects.{Metadata, ObjectId}

case class PreTransactionOpportunisticRebuild(objectId: ObjectId,
                                              requiredMetadata: Metadata,
                                              data: DataBuffer)
