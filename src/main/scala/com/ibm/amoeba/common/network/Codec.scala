package com.ibm.amoeba.common.network

import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import com.ibm.amoeba.codec
import com.ibm.amoeba.codec.ObjectReadError
import com.ibm.amoeba.common.HLCTimestamp
import com.ibm.amoeba.common.ida.{IDA, ReedSolomon, Replication}
import com.ibm.amoeba.common.objects.{ByteArrayKeyOrdering, ByteRange, DataObjectPointer, FullObject, IntegerKeyOrdering, Key, KeyOrdering, KeyRange, KeyValueObjectPointer, LargestKeyLessThan, LargestKeyLessThanOrEqualTo, LexicalKeyOrdering, MetadataOnly, ObjectId, ObjectPointer, ObjectRefcount, ObjectRevision, ObjectType, ReadError, SingleKey}
import com.ibm.amoeba.common.paxos.ProposalId
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.{StoreId, StorePointer}
import com.ibm.amoeba.common.transaction.KeyValueUpdate.KeyRevision
import com.ibm.amoeba.common.transaction.{DataUpdate, DataUpdateOperation, FinalizationActionId, KeyValueUpdate, LocalTimeRequirement, ObjectUpdate, PreTransactionOpportunisticRebuild, RefcountUpdate, RevisionLock, SerializedFinalizationAction, TransactionDescription, TransactionDisposition, TransactionId, TransactionRequirement, TransactionStatus, VersionBump}

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
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
    case f => throw new EncodingError(f"Invalid key comparison: $f")


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
      throw new EncodingError("Unknown IDA")


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
    case t => throw new EncodingError(f"Invalid ObjecType: $t")


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
    case f => throw new EncodingError(f"Invalid Transaction Status: $f")


  def encodeTransactionDisposition(o: TransactionDisposition.Value): codec.TransactionDisposition = o match
    case TransactionDisposition.Undetermined => codec.TransactionDisposition.TRANSACTION_DISPOSITION_UNDETERMINED
    case TransactionDisposition.VoteCommit => codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_COMMIT
    case TransactionDisposition.VoteAbort => codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_ABORT

  def decodeTransactionDisposition(m: codec.TransactionDisposition): TransactionDisposition.Value = m match
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_UNDETERMINED => TransactionDisposition.Undetermined
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_COMMIT => TransactionDisposition.VoteCommit
    case codec.TransactionDisposition.TRANSACTION_DISPOSITION_VOTE_ABORT => TransactionDisposition.VoteAbort
    case f => throw new EncodingError(f"Invalid Transaction Disposition: $f")


  def encodeDataUpdateOperation(o: DataUpdateOperation.Value): codec.DataUpdateOperation = o match
    case DataUpdateOperation.Append => codec.DataUpdateOperation.DATA_UPDATE_OPERATION_APPEND
    case DataUpdateOperation.Overwrite => codec.DataUpdateOperation.DATA_UPDATE_OPERATION_OVERWRITE

  def decodeDataUpdateOperation(m: codec.DataUpdateOperation): DataUpdateOperation.Value = m match
    case codec.DataUpdateOperation.DATA_UPDATE_OPERATION_APPEND => DataUpdateOperation.Append
    case codec.DataUpdateOperation.DATA_UPDATE_OPERATION_OVERWRITE => DataUpdateOperation.Overwrite
    case f => throw new EncodingError(f"Invalid DataUpdateOperation: $f")


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
      case f => throw new EncodingError(f"Invalid KeyRequirement: $f")


  def encode(o: KeyRevision): codec.KeyRevision =
    codec.KeyRevision.newBuilder()
      .setKey(ByteString.copyFrom(o.key.bytes))
      .setRevision(encode(o.revision))
      .build

  def decode(m: codec.KeyRevision): KeyRevision =
    KeyRevision(Key(m.getKey.toByteArray), decode(m.getRevision))


  def encode(o: KeyValueUpdate): codec.KeyValueUpdate =
    val builder = codec.KeyValueUpdate.newBuilder()
      .setObjectPointer(encode(o.objectPointer))

    o.requiredRevision.foreach: rev =>
      builder.setRequiredRevision(encode(rev))

    o.contentLock.foreach: lck =>
      lck.fullContents.foreach: kr =>
        builder.addContentLock(encode(kr))

    o.requirements.foreach: kr =>
      builder.addRequirements(encode(kr))

    builder.build

  def decode(m: codec.KeyValueUpdate): KeyValueUpdate =
    val objectPointer = decode(m.getObjectPointer).asInstanceOf[KeyValueObjectPointer]

    val requiredRevision = if m.hasRequiredRevision then
      Some(decode(m.getRequiredRevision))
    else
      None

    val contentLock = if m.getContentLockCount == 0 then
      None
    else
      Some(KeyValueUpdate.FullContentLock(m.getContentLockList.asScala.map(decode).toList))

    val requirements = m.getRequirementsList.asScala.map(decode).toList

    KeyValueUpdate(objectPointer, requiredRevision, contentLock, requirements)


  def encodeLocalTimeRequirementEnum(o: LocalTimeRequirement.Requirement.Value): codec.LocalTimeRequirementEnum = o match
    case LocalTimeRequirement.Requirement.LessThan => codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_LESS_THAN
    case LocalTimeRequirement.Requirement.GreaterThan => codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_GREATER_THAN
    case LocalTimeRequirement.Requirement.Equals => codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_EQUALS

  def decodeLocalTimeRequirementEnum(m: codec.LocalTimeRequirementEnum): LocalTimeRequirement.Requirement.Value = m match
    case codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_LESS_THAN => LocalTimeRequirement.Requirement.LessThan
    case codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_GREATER_THAN => LocalTimeRequirement.Requirement.GreaterThan
    case codec.LocalTimeRequirementEnum.LOCAL_TIME_REQUIREMENT_ENUM_EQUALS => LocalTimeRequirement.Requirement.Equals
    case f => throw new EncodingError(f"Invalid LocalTimeRequirementEnum: $f")


  def encode(o: LocalTimeRequirement): codec.LocalTimeRequirement =
    codec.LocalTimeRequirement.newBuilder()
      .setTimestamp(o.timestamp.asLong)
      .setRequirement(encodeLocalTimeRequirementEnum(o.tsRequirement))
      .build

  def decode(m: codec.LocalTimeRequirement): LocalTimeRequirement =
    LocalTimeRequirement(HLCTimestamp(m.getTimestamp), decodeLocalTimeRequirementEnum(m.getRequirement))


  def encode(o: TransactionRequirement): codec.TransactionRequirement =
    val builder = codec.TransactionRequirement.newBuilder()
    o match
      case tr: DataUpdate => builder.setDataUpdate(encode(tr))
      case tr: RefcountUpdate => builder.setRefcountUpdate(encode(tr))
      case tr: VersionBump => builder.setVersionBump(encode(tr))
      case tr: RevisionLock => builder.setRevisionLock(encode(tr))
      case tr: KeyValueUpdate => builder.setKvUpdate(encode(tr))
      case tr: LocalTimeRequirement => builder.setLocaltime(encode(tr))
    builder.build

  def decode(m: codec.TransactionRequirement): TransactionRequirement =
    if m.hasDataUpdate then decode(m.getDataUpdate)
    else if m.hasRefcountUpdate then decode(m.getRefcountUpdate)
    else if m.hasVersionBump then decode(m.getVersionBump)
    else if m.hasRevisionLock then decode(m.getRevisionLock)
    else if m.hasKvUpdate then decode(m.getKvUpdate)
    else if m.hasLocaltime then decode(m.getLocaltime)
    else throw new EncodingError("Unknown Transaction Requirement")


  def encode(o: SerializedFinalizationAction): codec.SerializedFinalizationAction =
    codec.SerializedFinalizationAction.newBuilder()
      .setTypeUuid(encodeUUID(o.typeId.uuid))
      .setData(ByteString.copyFrom(o.data))
      .build

  def decode(m: codec.SerializedFinalizationAction): SerializedFinalizationAction =
    SerializedFinalizationAction(FinalizationActionId(decodeUUID(m.getTypeUuid)), m.getData.toByteArray)


  def encode(o: StoreId): codec.StoreId =
    codec.StoreId.newBuilder()
      .setStoragePoolUuid(encodeUUID(o.poolId.uuid))
      .setStoragePoolIndex(o.poolIndex)
      .build()

  def decode(m: codec.StoreId): StoreId =
    StoreId(PoolId(decodeUUID(m.getStoragePoolUuid)), m.getStoragePoolIndex.toByte)


  def encode(o: TransactionDescription): codec.TransactionDescription =
    val builder = codec.TransactionDescription.newBuilder()

    builder.setTransactionUuid(encodeUUID(o.transactionId.uuid))
    builder.setStartTimestamp(o.startTimestamp.asLong)
    builder.setPrimaryObject(encode(o.primaryObject))
    builder.setDesignatedLeaderUid(o.designatedLeaderUID)
    o.requirements.foreach: tr =>
      builder.addRequirements(encode(tr))
    o.finalizationActions.foreach: sfa =>
      builder.addFinalizationActions(encode(sfa))
    o.originatingClient.foreach: clientId =>
      builder.setOriginatingClient(encodeUUID(clientId.uuid))
    o.notifyOnResolution.foreach: storeId =>
      builder.addNotifyOnResolution(encode(storeId))
    o.notes.foreach: s =>
      builder.addNotes(s)

    builder.build
  def decode(m: codec.TransactionDescription): TransactionDescription =
    val txuuid = TransactionId(decodeUUID(m.getTransactionUuid))
    val startTs = HLCTimestamp(m.getStartTimestamp)
    val primaryObj = decode(m.getPrimaryObject)
    val designatedLeader = m.getDesignatedLeaderUid.toByte
    val requirements = m.getRequirementsList.asScala.map(decode).toList
    val serializedFas = m.getFinalizationActionsList.asScala.map(decode).toList
    val origClient = if m.hasOriginatingClient then Some(ClientId(decodeUUID(m.getOriginatingClient))) else None
    val notifyOnRes = m.getNotifyOnResolutionList.asScala.map(decode).toList
    val notes = m.getNotesList.asScala.toList

    TransactionDescription(txuuid, startTs, primaryObj, designatedLeader, requirements,
      serializedFas, origClient, notifyOnRes, notes)


  def encode(o: ProposalId): codec.ProposalId =
    codec.ProposalId.newBuilder()
      .setNumber(o.number)
      .setUid(o.peer)
      .build

  def decode(m: codec.ProposalId): ProposalId =
    ProposalId(m.getNumber, m.getUid.toByte)


  def encode(o: TxPrepare): codec.TxPrepare =
    codec.TxPrepare.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTxd(encode(o.txd))
      .setProposalId(encode(o.proposalId))
      .build

  def decode(m: codec.TxPrepare,
             objectUpdates: List[ObjectUpdate],
             preTxRebuilds: List[PreTransactionOpportunisticRebuild]): TxPrepare =
    TxPrepare(decode(m.getTo), decode(m.getFrom), decode(m.getTxd), decode(m.getProposalId),
      objectUpdates, preTxRebuilds)


  def encode(o: TxPrepareResponse): codec.TxPrepareResponse =
    val builder = codec.TxPrepareResponse.newBuilder()

    builder.setTo(encode(o.to))
    builder.setFrom(encode(o.from))
    builder.setTransactionUuid(encodeUUID(o.transactionId.uuid))
    val prt = o.response match
      case Left(nack) =>
        builder.setPromisedId(encode(nack.promisedId))
        codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_NACK
      case Right(promise) =>
        promise.lastAccepted.foreach: (pid, value) =>
          builder.setLastAcceptedId(encode(pid))
          builder.setLastAcceptedValue(value)
        codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_PROMISE
    builder.setResponseType(prt)
    builder.setDisposition(encodeTransactionDisposition(o.disposition))

    val arr = new Array[Byte](o.collisions.size * 16)

    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    o.collisions.foreach: id =>
      bb.putLong(id.uuid.getMostSignificantBits)
      bb.putLong(id.uuid.getLeastSignificantBits)

    builder.setTransactionCollisions(ByteString.copyFrom(arr))

    builder.build

  def decode(m: codec.TxPrepareResponse): TxPrepareResponse =
    val to = decode(m.getTo)
    val from = decode(m.getFrom)
    val txid = TransactionId(decodeUUID(m.getTransactionUuid))
    val response = m.getResponseType match
      case codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_NACK =>
        Left(TxPrepareResponse.Nack(decode(m.getPromisedId)))
      case codec.TxPrepareResponseType.TX_PREPARE_RESPONSE_TYPE_PROMISE =>
        val la = if m.hasLastAcceptedId then
          Some((decode(m.getLastAcceptedId), m.getLastAcceptedValue))
        else
          None
        Right(TxPrepareResponse.Promise(la))
      case f => throw new EncodingError("Unknown Prepare Response Type")
    val proposalId = decode(m.getProposalId)
    val disposition = decodeTransactionDisposition(m.getDisposition)
    var collisions = List[TransactionId]()

    val bb = m.getTransactionCollisions.asReadOnlyByteBuffer()
    bb.order(ByteOrder.BIG_ENDIAN)
    while (bb.remaining() != 0) {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      collisions = TransactionId(new UUID(msb, lsb)) :: collisions
    }

    TxPrepareResponse(to, from, txid, response, proposalId, disposition, collisions)


  def encode(o: TxAccept): codec.TxAccept =
    codec.TxAccept.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setProposalId(encode(o.proposalId))
      .setValue(o.value)
      .build

  def decode(m: codec.TxAccept): TxAccept =
    TxAccept(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      decode(m.getProposalId), m.getValue)


  def encode(o: TxAcceptResponse): codec.TxAcceptResponse =
    val builder = codec.TxAcceptResponse.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setProposalId(encode(o.proposalId))

      o.response match
        case Left(nack) =>
          builder.setIsNack(true)
          builder.setPromisedId(encode(nack.promisedId))
        case Right(accepted) =>
          builder.setValue(accepted.value)

      builder.build

  def decode(m: codec.TxAcceptResponse): TxAcceptResponse =
    val response = if m.getIsNack then
      Left(TxAcceptResponse.Nack(decode(m.getPromisedId)))
    else
      Right(TxAcceptResponse.Accepted(m.getValue))

    TxAcceptResponse(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      decode(m.getProposalId), response)


  def encode(o: TxResolved): codec.TxResolved =
    codec.TxResolved.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setCommitted(o.committed)
      .build

  def decode(m: codec.TxResolved): TxResolved =
    TxResolved(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      m.getCommitted)


  def encode(o: TxCommitted): codec.TxCommitted =
    val builder = codec.TxCommitted.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))

    val arr = new Array[Byte](o.objectCommitErrors.size * 16)

    val bb = ByteBuffer.wrap(arr)
    bb.order(ByteOrder.BIG_ENDIAN)
    o.objectCommitErrors.foreach: id =>
      bb.putLong(id.uuid.getMostSignificantBits)
      bb.putLong(id.uuid.getLeastSignificantBits)

    builder.setObjectCommitErrors(ByteString.copyFrom(arr))

    builder.build

  def decode(m: codec.TxCommitted): TxCommitted =

    var commitErrors = List[ObjectId]()

    val bb = m.getObjectCommitErrors.asReadOnlyByteBuffer()
    bb.order(ByteOrder.BIG_ENDIAN)
    while (bb.remaining() != 0) {
      val msb = bb.getLong()
      val lsb = bb.getLong()
      commitErrors = ObjectId(new UUID(msb, lsb)) :: commitErrors
    }

    TxCommitted(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      commitErrors)


  def encode(o: TxFinalized): codec.TxFinalized =
    codec.TxFinalized.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setCommitted(o.committed)
      .build

  def decode(m: codec.TxFinalized): TxFinalized =
    TxFinalized(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      m.getCommitted)


  def encode(o: TxHeartbeat): codec.TxHeartbeat =
    codec.TxHeartbeat.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .build

  def decode(m: codec.TxHeartbeat): TxHeartbeat =
    TxHeartbeat(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)))


  def encode(o: TxStatusRequest): codec.TxStatusRequest =
    codec.TxStatusRequest.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setRequestUuid(encodeUUID(o.requestUUID))
      .build

  def decode(m: codec.TxStatusRequest): TxStatusRequest =
    TxStatusRequest(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      decodeUUID(m.getRequestUuid))


  def encode(o: TxStatusResponse): codec.TxStatusResponse =
    val builder = codec.TxStatusResponse.newBuilder()
      .setTo(encode(o.to))
      .setFrom(encode(o.from))
      .setTransactionUuid(encodeUUID(o.transactionId.uuid))
      .setRequestUuid(encodeUUID(o.requestUUID))

    val status = o.status match
      case None =>
        builder.setHaveStatus(false)

      case Some(stat) =>
        builder.setHaveStatus(true)
        builder.setIsFinalized(stat.finalized)
        builder.setStatus(encodeTransactionStatus(stat.status))

    builder.build

  def decode(m: codec.TxStatusResponse): TxStatusResponse =
    val status = if m.getHaveStatus then
      Some(TxStatusResponse.TxStatus(decodeTransactionStatus(m.getStatus), m.getIsFinalized))
    else
      None

    TxStatusResponse(decode(m.getTo), decode(m.getFrom), TransactionId(decodeUUID(m.getTransactionUuid)),
      decodeUUID(m.getRequestUuid), status)


  def encodeReadError(o: ReadError.Value): codec.ObjectReadError = o match
    case ReadError.ObjectMismatch => codec.ObjectReadError.OBJECT_READ_ERROR_OBJECT_MISMATCH
    case ReadError.ObjectNotFound => codec.ObjectReadError.OBJECT_READ_ERROR_OBJECT_NOT_FOUND
    case ReadError.StoreNotFound => codec.ObjectReadError.OBJECT_READ_ERROR_STORE_NOT_FOUND
    case ReadError.CorruptedObject => codec.ObjectReadError.OBJECT_READ_ERROR_CORRUPTED_OBJECT

  def decodeReadError(m: codec.ObjectReadError): ReadError.Value = m match
    case codec.ObjectReadError.OBJECT_READ_ERROR_OBJECT_MISMATCH => ReadError.ObjectMismatch
    case codec.ObjectReadError.OBJECT_READ_ERROR_OBJECT_NOT_FOUND => ReadError.ObjectNotFound
    case codec.ObjectReadError.OBJECT_READ_ERROR_STORE_NOT_FOUND => ReadError.StoreNotFound
    case codec.ObjectReadError.OBJECT_READ_ERROR_CORRUPTED_OBJECT => ReadError.CorruptedObject
    case f => throw new EncodingError(f"Invalid ObjectReadError: $f")


  def encode(o: Read): codec.Read =
    val builder = codec.Read.newBuilder()

    builder.setToStore(encode(o.toStore))
    builder.setFromClient(encodeUUID(o.fromClient.uuid))
    builder.setReadUuid(encodeUUID(o.readUUID))
    builder.setObjectPointer(encode(o.objectPointer))

    val readType = o.readType match
      case _: MetadataOnly =>
        codec.ReadType.READ_TYPE_METADATA_ONLY
      case _: FullObject =>
        codec.ReadType.READ_TYPE_FULL_OBJECT
      case rt: ByteRange =>
        builder.setOffset(rt.offset)
        builder.setLength(rt.length)
        codec.ReadType.READ_TYPE_BYTE_RANGE
      case rt: SingleKey =>
        builder.setKey(ByteString.copyFrom(rt.key.bytes))
        builder.setComparison(encodeKeyComparison(rt.ordering))
        codec.ReadType.READ_TYPE_SINGLE_KEY
      case rt: LargestKeyLessThan =>
        builder.setKey(ByteString.copyFrom(rt.key.bytes))
        builder.setComparison(encodeKeyComparison(rt.ordering))
        codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN
      case rt: LargestKeyLessThanOrEqualTo =>
        builder.setKey(ByteString.copyFrom(rt.key.bytes))
        builder.setComparison(encodeKeyComparison(rt.ordering))
        codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN_OR_EQUAL_TO
      case rt: KeyRange =>
        builder.setMin(ByteString.copyFrom(rt.minimum.bytes))
        builder.setMax(ByteString.copyFrom(rt.maximum.bytes))
        builder.setComparison(encodeKeyComparison(rt.ordering))
        codec.ReadType.READ_TYPE_KEY_RANGE

      builder.setReadType(readType)

    builder.build

  def decode(m: codec.Read): Read =
    val toStore = decode(m.getToStore)
    val from = ClientId(decodeUUID(m.getFromClient))
    val readUuid = decodeUUID(m.getReadUuid)
    val objPtr = decode(m.getObjectPointer)
    val readType = m.getReadType match
      case codec.ReadType.READ_TYPE_METADATA_ONLY => MetadataOnly()
      case codec.ReadType.READ_TYPE_FULL_OBJECT => FullObject()
      case codec.ReadType.READ_TYPE_BYTE_RANGE => ByteRange(m.getOffset, m.getLength)
      case codec.ReadType.READ_TYPE_SINGLE_KEY => SingleKey(Key(m.getKey.toByteArray), decodeKeyComparison(m.getComparison))
      case codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN => LargestKeyLessThan(Key(m.getKey.toByteArray), decodeKeyComparison(m.getComparison))
      case codec.ReadType.READ_TYPE_LARGEST_KEY_LESS_THAN_OR_EQUAL_TO => LargestKeyLessThanOrEqualTo(Key(m.getKey.toByteArray), decodeKeyComparison(m.getComparison))
      case codec.ReadType.READ_TYPE_KEY_RANGE => KeyRange(Key(m.getMin.toByteArray), Key(m.getMax.toByteArray), decodeKeyComparison(m.getComparison))
      case f => throw new EncodingError(f"Invalid Read Type: $f")

    Read(toStore, from, readUuid, objPtr, readType)
