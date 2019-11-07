package com.ibm.amoeba.common.store

import com.ibm.amoeba.common.objects.ObjectId

case class Locater(objectId: ObjectId, storePointer: StorePointer)
