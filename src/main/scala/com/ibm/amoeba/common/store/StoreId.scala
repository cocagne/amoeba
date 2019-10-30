package com.ibm.amoeba.common.store

import com.ibm.amoeba.common.pool.PoolId

case class StoreId(poolId: PoolId, poolIndex: Byte)
