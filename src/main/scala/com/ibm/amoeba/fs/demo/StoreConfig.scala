package com.ibm.amoeba.fs.demo

import java.io.{File, FileInputStream}
import java.util.UUID

import com.ibm.amoeba.common.util.YamlFormat._
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor


object StoreConfig {

  case class Store(poolUuid: UUID, index: Int, backend: StorageBackend)

  object Store extends YObject[Store]:
    val poolUuid: Required[UUID] = Required("pool-uuid", YUUID)
    val storeIndex: Required[Int] = Required("store-index", YInt)
    val backend: Required[RocksDB] = Required("backend", Choice("storage-engine", Map("rocksdb" -> RocksDB)))

    val attrs: List[Attr] = poolUuid :: storeIndex :: backend :: Nil

    def create(o: Object): Store = Store(poolUuid.get(o), storeIndex.get(o), backend.get(o))


  sealed abstract class StorageBackend

  case class RocksDB() extends StorageBackend

  object RocksDB extends YObject[RocksDB]:
    val attrs: List[Attr] = Nil

    def create(o: Object): RocksDB = RocksDB()


  def loadStore(file: File): Store =
    val yaml = new Yaml(new SafeConstructor)
    val y = yaml.load[java.util.AbstractMap[Object, Object]](new FileInputStream(file))
    Store.create(y)
}
