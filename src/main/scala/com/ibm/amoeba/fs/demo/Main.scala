package com.ibm.amoeba.fs.demo

import java.io.{File, StringReader}
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.Executors

import com.ibm.amoeba.AmoebaError
import com.ibm.amoeba.client.{AmoebaClient, KeyValueObjectState}
import com.ibm.amoeba.client.internal.SimpleAmoebaClient
import com.ibm.amoeba.client.internal.allocation.SinglePoolObjectAllocator
import com.ibm.amoeba.common.ida.{ReedSolomon, Replication}
import com.ibm.amoeba.common.network.{ClientId, ClientRequest, ClientResponse, TxMessage}
import com.ibm.amoeba.common.objects.{Key, KeyValueObjectPointer, ObjectRevisionGuard}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.util.{BackgroundTaskPool, YamlFormat}
import com.ibm.amoeba.fs.FileSystem
import com.ibm.amoeba.fs.demo.network.ZMQNetwork
import com.ibm.amoeba.fs.impl.simple.SimpleFileSystem
import com.ibm.amoeba.fs.nfs.AmoebaNFS
import com.ibm.amoeba.server.{RegisteredTransactionFinalizerFactory, SimpleDriverRecoveryMixin, StoreManager}
import com.ibm.amoeba.server.crl.sweeper.Sweeper
import com.ibm.amoeba.server.store.Bootstrap
import com.ibm.amoeba.server.store.backend.RocksDBBackend
import com.ibm.amoeba.server.store.cache.SimpleLRUObjectCache
import com.ibm.amoeba.server.transaction.SimpleTransactionDriver
import org.dcache.nfs.ExportFile
import org.dcache.nfs.v3.{MountServer, NfsServerV3}
import org.dcache.nfs.v3.xdr.{mount_prot, nfs3_prot}
import org.dcache.nfs.v4.NFSServerV41
import org.dcache.nfs.v4.xdr.nfs4_prot
import org.dcache.nfs.vfs.VirtualFileSystem
import org.dcache.oncrpc4j.rpc.{OncRpcProgram, OncRpcSvcBuilder}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}

object Main {

  val AmoebafsKey: Key = Key("amoeba")

  case class Args(mode:String="",
                  configFile:File=null,
                  log4jConfigFile: File=null,
                  nodeName:String="",
                  host:String="",
                  port:Int=0)

  class ConfigError(msg: String) extends AmoebaError(msg)

  class NetworkBridge {
    var oclient: Option[AmoebaClient] = None
    var onode: Option[StoreManager] = None

    def onClientResponseReceived(msg: ClientResponse): Unit ={
      oclient.foreach(_.receiveClientResponse(msg))
    }
    def onClientRequestReceived(msg: ClientRequest): Unit = {
      onode.foreach(_.receiveClientRequest(msg))
    }
    def onTransactionMessageReceived(msg: TxMessage): Unit = {
      onode.foreach(_.receiveTransactionMessage(msg))
    }
  }

  def setLog4jConfigFile(f: File): Unit = {
    //System.setProperty("log4j2.debug", "true")

    // Set all loggers to Asynchronous Logging
    System.setProperty("log4j2.contextSelector", "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector")
    System.setProperty("log4j2.configurationFile", s"file:${f.getAbsolutePath}")
  }

  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Args]("demo") {
      head("demo", "0.1")

      cmd("bootstrap").text("Bootstrap a new Amoeba system").
        action( (_,c) => c.copy(mode="bootstrap")).
        children(
          arg[File]("<config-file>").text("Configuration File").
            action( (x, c) => c.copy(configFile=x)).
            validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x"))
        )

      cmd("node").text("Starts an Amoeba Storage Node").
        action( (_,c) => c.copy(mode="node")).
        children(
          arg[File]("<config-file>").text("Configuration File").
            action( (x, c) => c.copy(configFile=x)).
            validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<node-name>").text("Storage Node Name").action((x,c) => c.copy(nodeName=x))
        )

      cmd("nfs").text("Launches a Amoeba NFS server").
        action( (_,c) => c.copy(mode="amoeba")).
        children(
          arg[File]("<config-file>").text("Amoeba Configuration File").
            action( (x, c) => c.copy(configFile=x)).
            validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[File]("<log4j-config-file>").text("Log4j Configuration File").
            action( (x, c) => c.copy(log4jConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Log4j Config file does not exist: $x")),
        )

      cmd("rebuild").text("Rebuilds a store").
        action( (_,c) => c.copy(mode="rebuild")).
        children(
          arg[File]("<config-file>").text("Configuration File").
            action( (x, c) => c.copy(configFile=x)).
            validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<store-name>").text("Data Store Name. Format is \"pool-name:storeNumber\"").
            action((x,c) => c.copy(nodeName=x)).
            validate { x =>
              val arr = x.split(":")
              if (arr.length == 2) {
                try {
                  Integer.parseInt(arr(1))
                  success
                } catch {
                  case _: Throwable => failure("Store name must match the format \"pool-name:storeNumber\"")
                }
              }
              else failure("Store name must match the format \"pool-name:storeNumber\"")
            }
        )

      checkConfig( c => if (c.mode == "") failure("Invalid command") else success )
    }

    parser.parse(args, Args()) match {
      case Some(cfg) =>
        //
        try {
          val config = ConfigFile.loadConfig(cfg.configFile)
          println(s"Successful config: $cfg")
          //println(s"Config file: $config")
          cfg.mode match {
            case "bootstrap" => bootstrap(config)
            case "node" => node(cfg.nodeName, config)
            case "amoeba" => amoeba_server(cfg.log4jConfigFile, config)
            //case "rebuild" => rebuild(cfg.nodeName, config)
          }
        } catch {
          case e: YamlFormat.FormatError => println(s"Error loading config file: $e")
          case e: ConfigError => println(s"Error: $e")
        }
      case None =>
    }
  }

  def createNetwork(cfg:ConfigFile.Config,
                    storageNode: Option[(String, String, Int)],
                    oclientId: Option[ClientId]): (NetworkBridge, ZMQNetwork) = {
    val b = new NetworkBridge
    val nodes = cfg.nodes.map(ds => ds._2.name -> (ds._2.endpoint.host, ds._2.endpoint.port))
    val stores = cfg.nodes.flatMap { t =>
      val (nodeName, node) = t
      node.stores.map { ds =>
        val poolUUID = cfg.pools(ds.pool).uuid
        StoreId(PoolId(poolUUID), ds.store.asInstanceOf[Byte]) -> nodeName
      }
    }
    val heartbeatPeriod = Duration(5, SECONDS)
    (b, new ZMQNetwork(oclientId, nodes, stores, storageNode, heartbeatPeriod,
      b.onClientResponseReceived,
      b.onClientRequestReceived,
      b.onTransactionMessageReceived))
  }

  def createAmoebaClient(cfg: ConfigFile.Config,
                         onnet: Option[(NetworkBridge, ZMQNetwork)]=None): (AmoebaClient, ZMQNetwork, KeyValueObjectPointer) = {
    val nucleus = cfg.onucleus.getOrElse(throw new ConfigError("Nucleus Pointer is missing from the config file!"))

    val (networkBridge, nnet) = onnet.getOrElse(createNetwork(cfg, None, None))

    val txStatusCacheDuration = Duration(10, SECONDS)
    val initialReadDelay = Duration(1, SECONDS)
    val maxReadDelay = Duration(3, SECONDS)
    val txRetransmitDelay = Duration(1, SECONDS)
    val allocationRetransmitDelay = Duration(5, SECONDS)

    val sched = Executors.newScheduledThreadPool(3)
    val ec: ExecutionContext = ExecutionContext.fromExecutorService(sched)

    val ret = (new SimpleAmoebaClient(nnet.clientMessenger, nnet.clientId, ec, nucleus,
      txStatusCacheDuration,
      initialReadDelay,
      maxReadDelay,
      txRetransmitDelay,
      allocationRetransmitDelay), nnet, nucleus)

    networkBridge.oclient = Some(ret._1)

    ret
  }

  def initializeAmoeba(client: AmoebaClient,
                       nucleus: KeyValueObjectPointer,
                       numIndexNodeSegments: Int = 100,
                       fileSegmentSize:Int=1024*1024): Future[FileSystem] = {

    implicit val ec: ExecutionContext = client.clientContext

    def loadFileSystem(kvos: KeyValueObjectState): Future[FileSystem] = kvos.contents.get(AmoebafsKey) match {
      case Some(arr) =>
        println("Amoeba already created")
        SimpleFileSystem.load(client, KeyValueObjectPointer(arr.value.bytes), 3)

      case None =>
        println("Creating Amoeba")
        val guard = ObjectRevisionGuard(kvos.pointer, kvos.revision)
        client.getStoragePool(kvos.pointer.poolId).flatMap { pool =>
          val allocator = new SinglePoolObjectAllocator(client, pool, kvos.pointer.ida, None)
          SimpleFileSystem.bootstrap(client, guard, allocator)
        }
    }

    client.read(nucleus).flatMap(loadFileSystem)
  }

  def amoeba_server(log4jConfigFile: File, cfg: ConfigFile.Config): Unit = {
    setLog4jConfigFile(log4jConfigFile)

    val (client, network, nucleus) = createAmoebaClient(cfg)

    val networkThread = new Thread {
      override def run(): Unit = {
        network.enterEventLoop()
      }
    }
    networkThread.start()

    val f = initializeAmoeba(client, nucleus)

    val fs = Await.result(f, Duration(5000, MILLISECONDS))

    val exports = "/ 192.168.56.2(rw)\n"

    val sched = Executors.newScheduledThreadPool(10)
    val ec = ExecutionContext.fromExecutorService(sched)

    val vfs: VirtualFileSystem = new AmoebaNFS(fs, ec)

    val nfsSvc = new OncRpcSvcBuilder().
      withPort(2049).
      withTCP.
      withAutoPublish.
      withWorkerThreadIoStrategy.
      build

    val exportFile = new ExportFile(new StringReader(exports))

    val nfs4 = new NFSServerV41.Builder().
      withExportTable(exportFile).
      withVfs(vfs).
      //withOperationFactory(new MDSOperationFactory).
      build

    val nfs3 = new NfsServerV3(exportFile, vfs)
    val mountd = new MountServer(exportFile, vfs)

    //val portmapSvc = new OncRpcEmbeddedPortmap()

    nfsSvc.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), mountd)
    nfsSvc.register(new OncRpcProgram(nfs3_prot.NFS_PROGRAM, nfs3_prot.NFS_V3), nfs3)
    nfsSvc.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4)
    nfsSvc.start()

    println("Amoeba NFS server started...")

    Thread.currentThread.join()
  }



  def node(nodeName: String, cfg: ConfigFile.Config): Unit = {

    val sched = Executors.newScheduledThreadPool(3)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(sched)

    val node = cfg.nodes.getOrElse(nodeName, throw new ConfigError(s"Invalid node name $nodeName"))

    setLog4jConfigFile(node.log4jConfigFile)

    val sweeper = node.crl match {
      case b: ConfigFile.Sweeper =>
        new Sweeper(Paths.get(b.path), b.numStreams, b.fileSizeMb * 1024, b.windowSize)
    }

    val stores = node.stores.map { s =>
      val storeId = StoreId(PoolId(cfg.pools(s.pool).uuid), s.store.asInstanceOf[Byte])

      s.backend match {
        case b: ConfigFile.RocksDB =>
          println(s"Creating data store $storeId. Path ${b.path}")
          new RocksDBBackend(b.path, storeId, ec)
      }
    }

    val objectCacheFactory = () => new SimpleLRUObjectCache(100)

    val nodeEndpoint = Some(node.name, node.endpoint.host, node.endpoint.port)

    val (networkBridge, nnet) = createNetwork(cfg, nodeEndpoint, None)

    val (client, network, _) = createAmoebaClient(cfg, Some((networkBridge, nnet)))

    networkBridge.oclient = Some(client)

    val txFinalizerFactory = new RegisteredTransactionFinalizerFactory(client)
    val txHeartbeatPeriod = Duration(1, SECONDS)
    val txRetryDelay = Duration(100, MILLISECONDS) //
    val txRetryCap = Duration(3, SECONDS)
    //val allocHeartbeatPeriod   = Duration(3, SECONDS)
    //val allocTimeout           = Duration(4, SECONDS)
    //val allocStatusQueryPeriod = Duration(1, SECONDS)

    val nodeNet = nnet.serverMessenger

    val storeManager = new StoreManager(
      objectCacheFactory,
      nodeNet,
      new BackgroundTaskPool,
      sweeper,
      txFinalizerFactory,
      SimpleTransactionDriver.factory(txRetryDelay, txRetryCap),
      txHeartbeatPeriod,
      stores
    ) with SimpleDriverRecoveryMixin

    networkBridge.onode = Some(storeManager)

    val networkThread = new Thread {
      override def run(): Unit = {
        network.enterEventLoop()
      }
    }
    networkThread.start()

    storeManager.start()
  }

  def mkdirectory(p: Path): Unit = {
    Files.createDirectories(p)
  }

  def bootstrap(cfg: ConfigFile.Config): Unit = {

    //if (cfg.onucleus.isDefined)
    //  throw new ConfigError("Nucleus Pointer is defined. Bootstrap process is already complete!")

    val sched = Executors.newScheduledThreadPool(1)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(sched)

    val bootstrapStores = cfg.nodes.values.flatMap(n => n.stores).map { s =>

      val poolIndex: Byte = s.store.asInstanceOf[Byte]
      val dataStoreId = StoreId(PoolId(cfg.pools(s.pool).uuid), poolIndex)

      s.backend match {
        case b: ConfigFile.RocksDB =>
          println(s"Creating data store $dataStoreId. Path ${b.path}")
          // Ensure parent directory exists
          mkdirectory(Paths.get(b.path).getParent)
          new RocksDBBackend(b.path, dataStoreId, ec)
      }
    }.toList

    cfg.nodes.values.foreach { n =>
      n.crl match {
        case s: ConfigFile.Sweeper => mkdirectory(Paths.get(s.path))
      }
    }

    val bootstrapPoolIDA = cfg.allocaters("bootstrap-allocater").ida

    val nucleus = Bootstrap.initialize(bootstrapPoolIDA, bootstrapStores)

    // Print yaml representation of Radicle Pointer
    println("# NHucleus Pointer Definition")
    println("radicle:")
    println(s"    uuid:      ${nucleus.id}")
    println(s"    pool-uuid: ${nucleus.poolId}")
    nucleus.size.foreach(size => println(s"    size:      $size"))
    println("    ida:")
    nucleus.ida match {
      case ida: Replication =>
        println(s"        type:            replication")
        println(s"        width:           ${ida.width}")
        println(s"        write-threshold: ${ida.writeThreshold}")

      case _: ReedSolomon => throw new NotImplementedError
    }
    println("    store-pointers:")
    nucleus.storePointers.foreach { sp =>
      println(s"        - pool-index: ${sp.poolIndex}")
      if (sp.data.length > 0)
        println(s"          data: ${java.util.Base64.getEncoder.encodeToString(sp.data)}")
    }
  }
}
