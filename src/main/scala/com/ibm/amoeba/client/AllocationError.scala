package com.ibm.amoeba.client

import com.ibm.amoeba.AmoebaError
import com.ibm.amoeba.common.pool.PoolId

case class AllocationError(poolId: PoolId) extends AmoebaError

