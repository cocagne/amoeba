package com.ibm.amoeba.common.network

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import com.ibm.amoeba.codec
import com.ibm.amoeba.common.HLCTimestamp
import com.ibm.amoeba.common.ida.{IDA, ReedSolomon, Replication}
import com.ibm.amoeba.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, IntegerKeyOrdering, Key, KeyOrdering, KeyValueObjectPointer, LexicalKeyOrdering, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StorePointer
import com.ibm.amoeba.common.transaction.KeyValueUpdate.KeyRevision
import com.ibm.amoeba.common.transaction.{DataUpdate, DataUpdateOperation, KeyValueUpdate, RefcountUpdate, RevisionLock, TransactionDisposition, TransactionId, TransactionStatus, VersionBump}

import scala.jdk.CollectionConverters.*
import java.util.UUID

object Codec extends Logging:

  def encodeUUID(o: UUID): codec.UUID =
    codec.UUID.newBuilder()
      .setMostSigBits(o.getMostSignificantBits)
      .setLeastSigBits(o.getLeastSignificantBits)
      .build

  def decodeUUID(m: codec.UUID): UUID =
    val msb = m.getMostSigBits
    val lsb = m.getLeastSigBits
    new UUID(msb, lsb)


  def encodeKeyComparison(o: KeyOrdering): codec.KeyComparison = o match
    case ByteArrayKeyOrdering => codec.KeyComparison.KEY_COMPARISON_BYTE_ARRAY
    case IntegerKeyOrdering => codec.KeyComparison.KEY_COMPARISON_INTEGER
    case LexicalKeyOrdering => codec.KeyComparison.KEY_COMPARISON_LEXICAL

  def decodeKeyComparison(m: codec.KeyComparison): KeyOrdering = m match
    case codec.KeyComparison.KEY_COMPARISON_BYTE_ARRAY => ByteArrayKeyOrdering
    case codec.KeyComparison.KEY_COMPARISON_INTEGER => IntegerKeyOrdering
    case codec.KeyComparison.KEY_COMPARISON_LEXICAL => LexicalKeyOrdering
    case f => assert(false, f"Invalid key comparison: $f")


  def encode(o: Replication): codec.Replication =
    codec.Replication.newBuilder()
      .setWidth(o.width)
      .setWriteThreshold(o.writeThreshold)
      .build

  def decode(m: codec.Replication): Replication =
    Replication(m.getWidth, m.getWriteThreshold)


  def encode(o: ReedSolomon): codec.ReedSolomon =
    codec.ReedSolomon.newBuilder()
      .setWidth(o.width)
      .setRestoreThreshold(o.restoreThreshold)
      .setWriteThreshold(o.writeThreshold)
      .build

  def decode(m: codec.ReedSolomon): ReedSolomon =
    ReedSolomon(m.getWidth, m.getRestoreThreshold, m.getWriteThreshold)


  def encode(o: IDA): codec.IDA =
    val builder = codec.IDA.newBuilder()
    o match
      case r: Replication => builder.setReplication(encode(r))
      case r: ReedSolomon => builder.setReedSolomon(encode(r))
    builder.build

  def decode(m: codec.IDA): IDA =
    if m.hasReplication then
      decode(m.getReplication)
    else if m.hasReedSolomon then
      decode(m.getReedSolomon)
    else
      assert(false, "Invalid IDA")


  def encode(o: ObjectRevision): codec.ObjectRevision =
    codec.ObjectRevision.newBuilder()
      .setUuid(encodeUUID(o.lastUpdateTxUUID))
      .build

  def decode(m: codec.ObjectRevision): ObjectRevision =
    ObjectRevision(TransactionId(decodeUUID(m.getUuid)))


  def encode(o: ObjectRefcount): codec.ObjectRefcount =
    codec.ObjectRefcount.newBuilder()
      .setUpdateSerial(o.updateSerial)
      .setRefcount(o.count)
      .build

  def decode(m: codec.ObjectRefcount): ObjectRefcount =
    ObjectRefcount(m.getUpdateSerial.toInt, m.getRefcount)


  def encode(o: StorePointer): codec.StorePointer =
    codec.StorePointer.newBuilder()
      .setStoreIndex(o.poolIndex)
      .setData(ByteString.copyFrom(o.data))
      .build

  def decode(m: codec.StorePointer): StorePointer =
    StorePointer(m.getStoreIndex.toByte, m.getData.toByteArray)


  def encodeObjectType(o: ObjectType.Value): codec.ObjectType = o match
    case ObjectType.Data => codec.ObjectType.OBJECT_TYPE_DATA
    case ObjectType.KeyValue => codec.ObjectType.OBJECT_TYPE_KEYVALUE

  def decodeObjectType(m: codec.ObjectType): ObjectType.Value = m match
    case codec.ObjectType.OBJECT_TYPE_DATA => ObjectType.Data
    case codec.ObjectType.OBJECT_TYPE_KEYVALUE => ObjectType.KeyValue
    case t => assert(false, f"Invalid ObjecType: $t")


  def encode(o: ObjectPointer): codec.ObjectPointer =
    val builder = codec.ObjectPointer.newBuilder()
      .setUuid(encodeUUID(o.id.uuid))
      .setPoolUuid(encodeUUID(o.poolId.uuid))
      .setIda(encode(o.ida))
      .setObjectType(encodeObjectType(o.objectType))

    o.size.foreach: size =>
      builder.setSize(size)

    o.storePointers.foreach: ptr =>
      builder.addStorePointers(encode(ptr))

    builder.build

  def decode(m: codec.ObjectPointer): ObjectPointer =
    val uuid = decodeUUID(m.getUuid)
    val poolUuid = decodeUUID(m.getPoolUuid)
    val ida = decode(m.getIda)
    val objectType = decodeObjectType(m.getObjectType)
    val osize = if m.getSize == 0 then None else Some(m.getSize)
    val storePointers = m.getStorePointersList.asScala.map(sp => decode(sp)).toArray

    objectType match
      case ObjectType.Data => new DataObjectPointer(ObjectId(uuid), PoolId(poolUuid), osize, ida, storePointers)
      case ObjectType.KeyValue => new KeyValueObjectPointer(ObjectId(uuid), PoolId(poolUuid), osize, ida, storePointers)


  def encodeTransactionStatus(o: TransactionStatus.Value): codec.TransactionStatus = o match
    case TransactionStatus.Unresolved => codec.TransactionStatus.TRANSACTION_STATUS_UNRESOLVED
    case TransactionStatus.Committed => codec.TransactionStatus.TRANSACTION_STATUS_COMMITTED
    case TransactionStatus.Aborted => codec.TransactionStatus.TRANSACTION_STATUS_ABORT

  def decodeTransactionStatus(m: codec.TransactionStatus): TransactionStatus.Value = m match
    case codec.TransactionStatus.TRANSACTION_STATUS_UNRESOLVED => TransactionStatus.Unresolved
    case codec.TransactionStatus.TRANSACTION_STATUS_COMMITTED => TransactionStatus.Committed
    case codec.TransactionStatus.TRANSACTION_STATUS_ABORT => TransactionStatus.Aborted
    case f => assert(false, f"Invalid Transaction Status: $f")


  def encodeTransactionDisposition(o: TransactionDisposition.Value): codec.TransactionDisposition = o match
    case TransactionDisposition.Undetermined => codec.TransactionDisposition.TRANSACTION_DISPOSITION_UNDETERMINED
    case TransactionDisposition.VoteCommit => codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_COMMIT
    case TransactionDisposition.VoteAbort => codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_ABORT

  def decodeTransactionDisposition(m: codec.TransactionDisposition): TransactionDisposition.Value = m match
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_UNDETERMINED => TransactionDisposition.Undetermined
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_COMMIT => TransactionDisposition.VoteCommit
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_ABORT => TransactionDisposition.VoteAbort
    case f => assert(false, f"Invalid Transaction Disposition: $f")


  def encodeDataUpdateOperation(o: DataUpdateOperation.Value): codec.DataUpdateOperation = o match
    case DataUpdateOperation.Append => codec.DataUpdateOperation.DATA_UPDATE_OPERATION_APPEND
    case DataUpdateOperation.Overwrite => codec.DataUpdateOperation.DATA_UPDATE_OPERATION_OVERWRITE

  def decodeDataUpdateOperation(m: codec.DataUpdateOperation): DataUpdateOperation.Value = m match
    case codec.DataUpdateOperation.DATA_UPDATE_OPERATION_APPEND => DataUpdateOperation.Append
    case codec.DataUpdateOperation.DATA_UPDATE_OPERATION_OVERWRITE => DataUpdateOperation.Overwrite
    case f => assert(false, f"Invalid DataUpdateOperation: $f")


  def encode(o: DataUpdate): codec.DataUpdate =
    codec.DataUpdate.newBuilder()
      .setObjectPointer(encode(o.objectPointer))
      .setRequiredRevision(encode(o.requiredRevision))
      .setOperation(encodeDataUpdateOperation(o.operation))
      .build
    
  def decode(m: codec.DataUpdate): DataUpdate =
    DataUpdate(decode(m.getObjectPointer),
      decode(m.getRequiredRevision),
      decodeDataUpdateOperation(m.getOperation))


  def encode(o: RefcountUpdate): codec.RefcountUpdate =
    codec.RefcountUpdate.newBuilder()
      .setObjectPointer(encode(o.objectPointer))
      .setRequiredRefcount(encode(o.requiredRefcount))
      .setNewRefcount(encode(o.newRefcount))
      .build

  def decode(m: codec.RefcountUpdate): RefcountUpdate =
    RefcountUpdate(decode(m.getObjectPointer),
      decode(m.getRequiredRefcount),
      decode(m.getNewRefcount))


  def encode(o: VersionBump): codec.VersionBump =
    codec.VersionBump.newBuilder()
      .setObjectPointer(encode(o.objectPointer))
      .setRequiredRevision(encode(o.requiredRevision))
      .build

  def decode(m: codec.VersionBump): VersionBump =
    VersionBump(decode(m.getObjectPointer),
      decode(m.getRequiredRevision))


  def encode(o: RevisionLock): codec.RevisionLock =
    codec.RevisionLock.newBuilder()
      .setObjectPointer(encode(o.objectPointer))
      .setRequiredRevision(encode(o.requiredRevision))
      .build

  def decode(m: codec.RevisionLock): RevisionLock =
    RevisionLock(decode(m.getObjectPointer),
      decode(m.getRequiredRevision))


  def encode(o: KeyValueUpdate.KeyRequirement): codec.KVReq =
    val builder = codec.KVReq.newBuilder()
    builder.setKey(ByteString.copyFrom(o.key.bytes))

    val req = o match
      case r: KeyValueUpdate.KeyRevision =>
        builder.setRevision(encode(r.revision))
        codec.KeyRequirement.KEY_REQUIREMENT_KEY_REVISION

      case r: KeyValueUpdate.KeyObjectRevision =>
        builder.setRevision(encode(r.revision))
        codec.KeyRequirement.KEY_REQUIREMENT_KEY_OBJECT_REVISION

      case r: KeyValueUpdate.WithinRange =>
        builder.setComparison(encodeKeyComparison(r.ordering))
        codec.KeyRequirement.KEY_REQUIREMENT_WITHIN_RANGE

      case _: KeyValueUpdate.Exists => codec.KeyRequirement.KEY_REQUIREMENT_EXISTS
      case _: KeyValueUpdate.MayExist => codec.KeyRequirement.KEY_REQUIREMENT_MAY_EXIST
      case _: KeyValueUpdate.DoesNotExist => codec.KeyRequirement.KEY_REQUIREMENT_DOES_NOT_EXIST

      case r: KeyValueUpdate.TimestampEquals =>
        builder.setTimestamp(r.timestamp.asLong)
        codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_EQUALS

      case r: KeyValueUpdate.TimestampLessThan =>
        builder.setTimestamp(r.timestamp.asLong)
        codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_LESS_THAN

      case r: KeyValueUpdate.TimestampGreaterThan =>
        builder.setTimestamp(r.timestamp.asLong)
        codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_GREATER_THAN

    builder.setRequirement(req)
    builder.build

  def decode(m: codec.KVReq): KeyValueUpdate.KeyRequirement =
    val key = Key(m.getKey.toByteArray)
    val timestamp = HLCTimestamp(m.getTimestamp)

    m.getRequirement match
      case codec.KeyRequirement.KEY_REQUIREMENT_EXISTS => KeyValueUpdate.Exists(key)
      case codec.KeyRequirement.KEY_REQUIREMENT_MAY_EXIST => KeyValueUpdate.MayExist(key)
      case codec.KeyRequirement.KEY_REQUIREMENT_DOES_NOT_EXIST => KeyValueUpdate.DoesNotExist(key)
      case codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_EQUALS => KeyValueUpdate.TimestampEquals(key, timestamp)
      case codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_LESS_THAN => KeyValueUpdate.TimestampLessThan(key, timestamp)
      case codec.KeyRequirement.KEY_REQUIREMENT_TIMESTAMP_GREATER_THAN => KeyValueUpdate.TimestampGreaterThan(key, timestamp)
      case codec.KeyRequirement.KEY_REQUIREMENT_KEY_REVISION =>
        val rev = decode(m.getRevision)
        KeyValueUpdate.KeyRevision(key, rev)
      case codec.KeyRequirement.KEY_REQUIREMENT_KEY_OBJECT_REVISION =>
        val rev = decode(m.getRevision)
        KeyValueUpdate.KeyObjectRevision(key, rev)
      case codec.KeyRequirement.KEY_REQUIREMENT_WITHIN_RANGE =>
        val ord = decodeKeyComparison(m.getComparison)
        KeyValueUpdate.WithinRange(key, ord)

      case f => assert(false, f"Invalid KeyRequirement: $f")


  def encode(o: KeyRevision): codec.KeyRevision =
    codec.KeyRevision.newBuilder()
      .setKey(ByteString.copyFrom(o.key.bytes))
      .setRevision(encode(o.revision))
      .build

  def decode(m: codec.KeyRevision): KeyRevision =
    KeyRevision(Key(m.getKey.toByteArray), decode(m.getRevision))



