package com.ibm.amoeba.common.objects

import com.ibm.amoeba.common.HLCTimestamp

case class ValueObject(value: Value,
                       revision: ObjectRevision,
                       timestamp: HLCTimestamp)
