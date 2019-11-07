package com.ibm.amoeba.common.store

import com.ibm.amoeba.common.objects.Key

import scala.collection.immutable.{HashMap, HashSet}

class KVObjectState {
  var min: Option[Key] = None
  var max: Option[Key] = None
  var left: Option[Key] = None
  var right: Option[Key] = None
  var content: HashMap[Key, ValueState] = new HashMap
  var noExistenceLocks: HashSet[Key] = new HashSet
}
