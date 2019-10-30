package com.ibm.amoeba.common.objects

import com.ibm.amoeba.common.transaction.KeyValueUpdate

sealed abstract class AllocationRevisionGuard

case class ObjectRevisionGuard( pointer: ObjectPointer,
                                requiredRevision: ObjectRevision) extends AllocationRevisionGuard

case class KeyRevisionGuard(
                           pointer: KeyValueObjectPointer,
                           key: Key,
                           requirement: KeyValueUpdate.TimestampRequirement.Value
                           ) extends AllocationRevisionGuard
