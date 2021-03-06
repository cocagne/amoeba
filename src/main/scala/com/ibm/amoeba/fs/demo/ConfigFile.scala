package com.ibm.amoeba.fs.demo

import java.io.{File, FileInputStream}
import java.util.UUID

import com.ibm.amoeba.common.Nucleus
import com.ibm.amoeba.common.ida.{IDA, Replication}
import com.ibm.amoeba.common.objects.{KeyValueObjectPointer, ObjectId}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StorePointer
import com.ibm.amoeba.common.util.YamlFormat._
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor

/*
 pools:
  - name: bootstrap-pool
    width: 5
    uuid: 65da619d-8e80-47dd-be70-98c3776ce0cd

  - name: inode-pool
    width: 5

  - name: file-data-pool
    width: 5

object-allocaters:
  - name: inode-allocater
    pool: inode-pool
    ida:
       type: replication
       width: 5
       writeThreshold: 3

storage-nodes:
  - name: node_a
    endpoint: tcp://127.0.0.1:5001
    crl:
       type: rocksdb
       path  /var/lib/aspen/node_a/crl
    stores:
       - pool: bootstrap
         store: 0
         type: rocksdb
         path:  /var/lib/aspen/node_a/bootstrap-0*/
object ConfigFile {

  object ReplicationFormat extends YObject[IDA] {
    val width: Required[Int]          = Required("width",            YInt)
    val writeThreshold: Required[Int] = Required("write-threshold",  YInt)

    val attrs: List[Attr] = width :: writeThreshold :: Nil

    def create(o: Object): IDA = Replication(width.get(o), writeThreshold.get(o))
  }

  val IDAOptions =  Map("replication" -> ReplicationFormat)


  case class Pool(name: String, width: Int, uuid: UUID)

  object Pool extends YObject[Pool] {
    val name: Required[String]                              = Required("name",  YString)
    val width: Required[Int]                                = Required("width", YInt)
    val uuid: Required[UUID]                                = Required("uuid",  YUUID)

    val attrs: List[Attr] = name :: width :: uuid :: Nil

    def create(o: Object): Pool = Pool(name.get(o), width.get(o), uuid.get(o))
  }

  case class ObjectAllocater(name: String, pool: String, uuid: UUID, ida: IDA, maxObjectSize: Option[Int])

  object ObjectAllocater extends YObject[ObjectAllocater] {
    val name: Required[String]       = Required("name", YString)
    val pool: Required[String]       = Required("pool", YString)
    val uuid: Required[UUID]         = Required("uuid", YUUID)
    val ida: Required[IDA]           = Required("ida",  Choice("type", IDAOptions))
    val maxObjectSize: Optional[Int] = Optional("max-object-size", YInt)

    val attrs: List[Attr] = name :: pool :: uuid :: ida :: maxObjectSize :: Nil

    def create(o: Object): ObjectAllocater = ObjectAllocater(name.get(o), pool.get(o), uuid.get(o), ida.get(o), maxObjectSize.get(o))
  }

  sealed abstract class StorageBackend

  case class RocksDB(path: String) extends StorageBackend

  object RocksDB extends YObject[RocksDB] {
    val path: Required[String] = Required("path", YString)
    val attrs: List[Attr] = path :: Nil

    def create(o: Object): RocksDB = RocksDB(path.get(o))
  }

  sealed abstract class CRLBackend

  case class Sweeper(path: String,
                     numStreams: Int,
                     fileSizeMb: Int,
                     windowSize: Int) extends CRLBackend

  object Sweeper extends YObject[Sweeper] {
    val path: Required[String]      = Required("path", YString)
    val numStreams: Optional[Int]   = Optional("num-streams", YInt)
    val fileSize: Optional[Int]     = Optional("max-file-size-mb", YInt)
    val windowSize: Optional[Int]   = Optional("window-size", YInt)
    val attrs: List[Attr] = path :: numStreams :: fileSize :: windowSize :: Nil

    def create(o: Object): Sweeper = Sweeper(
      path.get(o),
      numStreams.get(o).getOrElse(3),
      fileSize.get(o).getOrElse(100),
      windowSize.get(o).getOrElse(50))
  }

  case class DataStore(pool: String, store: Int, backend: StorageBackend)

  object DataStore extends YObject[DataStore] {
    val pool: Required[String]     = Required("pool",    YString)
    val store: Required[Int]       = Required("store",   YInt)
    val backend: Required[RocksDB] = Required("backend", Choice("storage-engine", Map("rocksdb" -> RocksDB)))

    val attrs: List[Attr] = pool :: store :: backend :: Nil

    def create(o: Object): DataStore = DataStore(pool.get(o), store.get(o), backend.get(o))
  }

  case class Endpoint(host: String, port: Int)

  object Endpoint extends YObject[Endpoint] {
    val host: Required[String] = Required("host", YString)
    val port: Required[Int]    = Required("port", YInt)

    val attrs: List[Attr] = host :: port :: Nil

    def create(o: Object): Endpoint = Endpoint(host.get(o), port.get(o))
  }

  case class StorageNode(name: String, uuid: UUID, endpoint: Endpoint, log4jConfigFile: File, crl: CRLBackend, stores: List[DataStore])

  object StorageNode extends YObject[StorageNode] {
    val name: Required[String]            = Required("name",         YString)
    val uuid: Required[UUID]              = Required("uuid",         YUUID)
    val endpoint: Required[Endpoint]      = Required("endpoint",     Endpoint)
    val log4jConf: Required[File]         = Required("log4j-config", YFile)
    val crl: Required[Sweeper]            = Required("crl",          Choice("storage-engine", Map("sweeper" -> Sweeper)))
    val stores: Required[List[DataStore]] = Required("stores",       YList(DataStore))

    val attrs: List[Attr] = name :: uuid :: endpoint :: log4jConf :: crl :: stores :: Nil

    def create(o: Object): StorageNode = StorageNode(name.get(o), uuid.get(o), endpoint.get(o), log4jConf.get(o), crl.get(o), stores.get(o))
  }

  object YStorePointer  extends YObject[StorePointer] {
    val poolIndex: Required[Int] = Required("pool-index", YInt)
    val data: Optional[String]   = Optional("data",       YString)

    val attrs: List[Attr] = poolIndex :: data :: Nil

    def create(o: Object): StorePointer = {
      val idx = poolIndex.get(o).asInstanceOf[Byte]
      val arr = data.get(o) match {
        case None => new Array[Byte](0)
        case Some(s) => java.util.Base64.getDecoder.decode(s)
      }
      new StorePointer(idx, arr)
    }
  }

  object NucleusPointer extends YObject[KeyValueObjectPointer] {

    val ida: Required[IDA]                          = Required("ida",            Choice("type", Map("replication" -> ReplicationFormat)))
    val storePointers: Required[List[StorePointer]] = Required("store-pointers", YList(YStorePointer))

    val attrs: List[Attr] = ida :: storePointers :: Nil

    def create(o: Object): KeyValueObjectPointer = {
      KeyValueObjectPointer(Nucleus.objectId, Nucleus.poolId, None, ida.get(o), storePointers.get(o).toArray)
    }
  }

  def zeroeduuid(uuid: UUID): Boolean = uuid.getMostSignificantBits == 0 && uuid.getLeastSignificantBits == 0

  case class Config(pools: Map[String, Pool], allocaters: Map[String, ObjectAllocater], nodes: Map[String, StorageNode], onucleus: Option[KeyValueObjectPointer]) {
    // Validate config
    {
      allocaters.values.foreach { a =>
        if (!pools.contains(a.pool))
          throw new FormatError(s"Object Allocater ${a.name} references unknown pool ${a.pool}")
      }

      if (!allocaters.contains("bootstrap-allocater"))
        throw new FormatError("Missing Required Object Allocater: 'bootstrap-allocater'")

      if (!pools.contains("bootstrap-pool"))
        throw new FormatError("Missing Required Pool: 'bootstrap-pool'")

      if (!zeroeduuid(pools("bootstrap-pool").uuid))
        throw new FormatError("bootstrap-pool must use a zeroed UUID")

      if (allocaters("bootstrap-allocater").pool != "bootstrap-pool")
        throw new FormatError("bootstrap-allocater must use the bootstrap-pool")

      if (!zeroeduuid(allocaters("bootstrap-allocater").uuid))
        throw new FormatError("bootstrap-allocater must use a zeroed UUID")

      allocaters.values.foreach { a =>
        if (!pools.contains(a.pool))
          throw new FormatError(s"Allocater ${a.name} references unknown pool ${a.pool}")
      }

      nodes.values.foreach { n =>
        n.stores.foreach { s =>
          if (!pools.contains(s.pool))
            throw new FormatError(s"Storage Node ${n.name} references unknown pool ${s.pool}")
        }
      }

      val allStores = pools.values.foldLeft(Set[String]()) { (s, p) =>
        (0 until p.width).foldLeft(s)( (s, i) => s + s"${p.name}:$i" )
      }
      val ownedStores = nodes.values.foldLeft(Set[String]()) { (s, n) =>
        n.stores.foldLeft(s)( (s, st) => s + s"${st.pool}:${st.store}" )
      }
      val missing = allStores &~ ownedStores
      val extra = ownedStores &~ allStores

      if (missing.nonEmpty) throw new FormatError(s"Unowned DataStore(s): $missing")
      if (extra.nonEmpty) throw new FormatError(s"Undefined DataStore(s): $extra")

      val uuids = pools.values.map(p => p.uuid) ++ allocaters.values.map(a => a.uuid) ++ nodes.values.map(n => n.uuid)

      uuids.foldLeft(Set[UUID]()) { (s, u) =>
        if (s.contains(u) && !zeroeduuid(u))
          throw new FormatError(s"Duplicated UUID: $u")
        s + u
      }
    }
  }

  object Config extends YObject[Config] {
    val pools: Required[List[Pool]]                 = Required("pools",             YList(Pool))
    val allocaters: Required[List[ObjectAllocater]] = Required("object-allocaters", YList(ObjectAllocater))
    val nodes: Required[List[StorageNode]]          = Required("storage-nodes",     YList(StorageNode))
    val nucleus: Optional[KeyValueObjectPointer]    = Optional("nucleus",           NucleusPointer)

    val attrs: List[Attr] = pools :: allocaters :: nodes :: nucleus :: Nil

    def create(o: Object): Config = {
      Config(
        pools.get(o).map(p => p.name -> p).toMap,
        allocaters.get(o).map(p => p.name -> p).toMap,
        nodes.get(o).map(p => p.name -> p).toMap,
        nucleus.get(o))
    }
  }

  def loadConfig(file: File): Config = {
    val yaml = new Yaml(new SafeConstructor)
    val y = yaml.load[java.util.AbstractMap[Object,Object]](new FileInputStream(file))
    Config.create(y)
  }
}
