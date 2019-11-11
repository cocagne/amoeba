package com.ibm.amoeba.server.store

import com.ibm.amoeba.common.objects.ObjectId
import com.ibm.amoeba.common.store.StorePointer

case class Locater(objectId: ObjectId, storePointer: StorePointer)
