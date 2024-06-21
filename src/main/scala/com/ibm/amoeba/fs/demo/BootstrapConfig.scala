package com.ibm.amoeba.fs.demo

import java.io.{File, FileInputStream}
import java.util.UUID

import com.ibm.amoeba.common.ida.{IDA, Replication}
import com.ibm.amoeba.common.util.YamlFormat._
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor

object BootstrapConfig {

  object ReplicationFormat extends YObject[IDA]:
    val width: Required[Int]          = Required("width",            YInt)
    val writeThreshold: Required[Int] = Required("write-threshold",  YInt)

    val attrs: List[Attr] = width :: writeThreshold :: Nil

    def create(o: Object): IDA = Replication(width.get(o), writeThreshold.get(o))

  val IDAOptions =  Map("replication" -> ReplicationFormat)


  case class BootstrapIDA(ida: IDA, maxObjectSize: Option[Int])

  object BootstrapIDA extends YObject[BootstrapIDA]:
    val ida: Required[IDA]           = Required("ida",  Choice("type", IDAOptions))
    val maxObjectSize: Optional[Int] = Optional("max-object-size", YInt)

    val attrs: List[Attr] = ida :: maxObjectSize :: Nil

    def create(o: Object): BootstrapIDA = BootstrapIDA(ida.get(o), maxObjectSize.get(o))


  case class StorageNode(name: String, host: String, port: Int)

  object StorageNode extends YObject[StorageNode]:
    val name: Required[String] = Required("name", YString)
    val host: Required[String] = Required("host", YString)
    val port: Required[Int]    = Required("port", YInt)

    val attrs: List[Attr] = name :: host :: port :: Nil

    def create(o: Object): StorageNode = StorageNode(name.get(o), host.get(o), port.get(o))


  case class Config(bootstrapIDA: IDA, nodes: List[StorageNode]):
    // Validate config
    if nodes.length != bootstrapIDA.width then
      throw new FormatError("Number of nodes must exactly match the Bootstrap IDA width")

  object Config extends YObject[Config]:
    val bootstrapIDA: Required[IDA]        = Required("bootstrap-ida",           Choice("type", Map("replication" -> ReplicationFormat)))
    val nodes: Required[List[StorageNode]] = Required("bootstrap-storage-nodes", YList(StorageNode))

    val attrs: List[Attr] = bootstrapIDA :: nodes :: Nil

    def create(o: Object): Config = Config( bootstrapIDA.get(o), nodes.get(o) )


  def loadBootstrapConfig(file: File): Config = {
    val yaml = new Yaml(new SafeConstructor)
    val y = yaml.load[java.util.AbstractMap[Object,Object]](new FileInputStream(file))
    Config.create(y)
  }
}