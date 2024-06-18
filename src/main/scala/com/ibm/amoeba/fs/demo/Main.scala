package com.ibm.amoeba.fs.demo

import java.io.{File, StringReader}
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.Executors
import com.ibm.amoeba.AmoebaError
import com.ibm.amoeba.client.KeyValueObjectState.ValueState
import com.ibm.amoeba.client.{AmoebaClient, DataObjectState, Host, HostId, KeyValueObjectState, MetadataObjectState, ObjectAllocator, ObjectState, StoragePool}
import com.ibm.amoeba.client.internal.SimpleAmoebaClient
import com.ibm.amoeba.client.internal.allocation.SinglePoolObjectAllocator
import com.ibm.amoeba.client.tkvl.{KVObjectRootManager, KeyValueListNode, Root, SinglePoolNodeAllocator, TieredKeyValueList}
import com.ibm.amoeba.common.{DataBuffer, HLCTimestamp}
import com.ibm.amoeba.common.ida.{ReedSolomon, Replication}
import com.ibm.amoeba.common.network.{ClientId, ClientRequest, ClientResponse, TxMessage}
import com.ibm.amoeba.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Insert, Key, KeyValueObjectPointer, LexicalKeyOrdering, Metadata, ObjectId, ObjectPointer, ObjectRevisionGuard, ObjectType, Value}
import com.ibm.amoeba.common.pool.PoolId
import com.ibm.amoeba.common.store.StoreId
import com.ibm.amoeba.common.transaction.KeyValueUpdate
import com.ibm.amoeba.common.transaction.KeyValueUpdate.{DoesNotExist, KeyRequirement}
import com.ibm.amoeba.common.util.{BackgroundTaskPool, YamlFormat}
import com.ibm.amoeba.fs.FileSystem
import com.ibm.amoeba.fs.demo.network.ZMQNetwork
import com.ibm.amoeba.fs.impl.simple.SimpleFileSystem
import com.ibm.amoeba.fs.nfs.AmoebaNFS
import com.ibm.amoeba.server.crl.simple.SimpleCRL
import com.ibm.amoeba.server.{RegisteredTransactionFinalizerFactory, SimpleDriverRecoveryMixin, StoreManager}
import com.ibm.amoeba.server.store.Bootstrap
import com.ibm.amoeba.server.store.backend.{Backend, RocksDBBackend}
import com.ibm.amoeba.server.store.cache.SimpleLRUObjectCache
import com.ibm.amoeba.server.transaction.SimpleTransactionDriver
import org.dcache.nfs.ExportFile
import org.dcache.nfs.v3.{MountServer, NfsServerV3}
import org.dcache.nfs.v3.xdr.{mount_prot, nfs3_prot}
import org.dcache.nfs.v4.{MDSOperationExecutor, NFSServerV41}
import org.dcache.nfs.v4.xdr.nfs4_prot
import org.dcache.nfs.vfs.VirtualFileSystem
import org.dcache.oncrpc4j.rpc.{OncRpcProgram, OncRpcSvcBuilder}
import org.apache.logging.log4j.scala.Logging

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.concurrent.ExecutionContext.Implicits.global

/*
place this file in the head of the CLASSPATH
---- log4j.properties ----

# Set root logger level to DEBUG and its only appender to A1.
log4j.rootLogger=TRACE, stdout

# A1 is set to be a ConsoleAppender.
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n

---- log4j-conf.xml ----

<?xml version="1.0" encoding="UTF-8"?>
<Configuration>
    <Appenders>
        <Console name="Console">
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
    </Appenders>
    <appender name="stdout" class="org.apache.log4j.ConsoleAppender">
        <layout class="org.apache.log4j.PatternLayout">
            <param name="ConversionPattern" value="%d{yyyy-MM-dd HH:mm:ss} %p %m%n"/>
        </layout>
    </appender>
    <Loggers>
        <Root level="trace">
            <AppenderRef ref="Console"/>
        </Root>
        <logger name="org.dcache.oncrpc4j.rpc.OncRpcSvc" level="TRACE">
            <AppenderRef ref="Console"/>
        </logger>
    </Loggers>
</Configuration>

 */

object Main {

  val AmoebafsKey: Key = Key("amoeba")

  case class Args(mode:String="",
                  configFile:File=null,
                  log4jConfigFile: File=null,
                  nodeName:String="",
                  storeName:String="",
                  host:String="",
                  port:Int=0)

  class ConfigError(msg: String) extends AmoebaError(msg)

  class NetworkBridge extends Logging {
    var oclient: Option[AmoebaClient] = None
    var onode: Option[StoreManager] = None

    def onClientResponseReceived(msg: ClientResponse): Unit ={
      //logger.trace(s"**** Recieved ClientResponse: $msg. $oclient")
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
    // Set all loggers to Asynchronous Logging
    System.setProperty("log4j2.contextSelector", "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector")
    System.setProperty("log4j2.configurationFile", s"file:${f.getAbsolutePath}")
  }

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Args]("demo") {
      head("demo", "0.1")

      cmd("bootstrap").text("Bootstrap a new Amoeba system").
        action( (_,c) => c.copy(mode="bootstrap")).
        children(
          arg[File]("<config-file>").text("Configuration File").
            action( (x, c) => c.copy(configFile=x)).
            validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x"))
        )

      cmd("debug").text("Runs debugging code").
        action((_, c) => c.copy(mode = "debug")).
        children(
          arg[File]("<config-file>").text("Configuration File").
            action((x, c) => c.copy(configFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[File]("<log4j-config-file>").text("Log4j Configuration File").
            action( (x, c) => c.copy(log4jConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Log4j Config file does not exist: $x"))
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
          arg[File]("<log4j-config-file>").text("Log4j Configuration File").
            action( (x, c) => c.copy(log4jConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Log4j Config file does not exist: $x")),

          arg[File]("<config-file>").text("Configuration File").
            action( (x, c) => c.copy(configFile=x)).
            validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<store-name>").text("Data Store Name. Format is \"pool-name:storeNumber\"").
            action((x,c) => c.copy(storeName=x)).
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
            case "debug" => run_debug_code(cfg.log4jConfigFile, config)
            case "rebuild" => rebuild(cfg.log4jConfigFile, cfg.storeName, config)
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

    val hosts = cfg.nodes.map: (name, h) =>
      HostId(h.uuid) -> Host(HostId(h.uuid), name, h.endpoint.host, h.endpoint.port)


    val (networkBridge, nnet) = onnet.getOrElse(createNetwork(cfg, None, None))

    val txStatusCacheDuration = Duration(10, SECONDS)
    val initialReadDelay = Duration(5, SECONDS)
    val maxReadDelay = Duration(6, SECONDS)
    val txRetransmitDelay = Duration(1, SECONDS)
    val allocationRetransmitDelay = Duration(5, SECONDS)

    val sched = Executors.newScheduledThreadPool(3)
    val ec: ExecutionContext = ExecutionContext.fromExecutorService(sched)

    val ret = (new SimpleAmoebaClient(nnet.clientMessenger, nnet.clientId, ec, nucleus,
      txStatusCacheDuration,
      initialReadDelay,
      maxReadDelay,
      txRetransmitDelay,
      allocationRetransmitDelay,
      hosts),  nnet, nucleus)

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
          SimpleFileSystem.bootstrap(client, guard, allocator, kvos.pointer, AmoebafsKey)
        }
    }

    client.read(nucleus).flatMap(loadFileSystem)
  }

  def OLD_run_debug_code(log4jConfigFile: File, cfg: ConfigFile.Config): Unit = {
    println(s"LOG4J CONFIG $log4jConfigFile")
    setLog4jConfigFile(log4jConfigFile)

    val (client, network, nucleus) = createAmoebaClient(cfg)

    val networkThread = new Thread {
      override def run(): Unit = {
        network.ioThread()
      }
    }
    networkThread.start()

    implicit val ec: ExecutionContext = client.clientContext

    println("------------ Reading Nucleus ---------------")
    for
      kvos <- client.read(nucleus)
      _=println("------------ Getting Storage Pool---------------")
      pool <- client.getStoragePool(kvos.pointer.poolId)
      _=println("------------ New Transaction---------------")
      tx = client.newTransaction()
      _=println("------------ New Root Manager---------------")
      frootMgr <- KVObjectRootManager.createNewTree(client, kvos.pointer, Key(100), ByteArrayKeyOrdering,
        new SinglePoolNodeAllocator(client, kvos.pointer.poolId),
        Map(Key(0) -> Value(Array[Byte](1,2,3))))(tx)
      _=println("------------ New Root Manager Step 2---------------")

      _ <- tx.commit()

      rootMgr <- frootMgr

      tx = client.newTransaction()

      _=println("------------ New TKVL ---------------")
      tkvl = new TieredKeyValueList(client, rootMgr)

      _=println("------------ Setting Key(1) ---------------")
      _ <- tkvl.set(Key(2), Value(Array[Byte](1,2,3)))(tx)

      _=println("------------ Committing! ---------------")
      _ <- tx.commit()
      _=println("------------ Commit Complete! ---------------")

      //guard = ObjectRevisionGuard(kvos.pointer, kvos.revision)
      //allocator = new SinglePoolObjectAllocator(client, pool, kvos.pointer.ida, None)
      //alloc <- allocator.allocateDataObject(guard, Array[Byte](0,1,2,3))(tx)
      //_ = tx.overwrite(kvos, tx.revision, rootDirInode.toArray) // ensure Tx has an object to modify

    yield
      ()
  }

  def run_debug_code(log4jConfigFile: File, cfg: ConfigFile.Config): Unit = {
    println(s"LOG4J CONFIG $log4jConfigFile")
    setLog4jConfigFile(log4jConfigFile)

    val (client, network, nucleus) = createAmoebaClient(cfg)

    val networkThread = new Thread {
      override def run(): Unit = {
        network.ioThread()
      }
    }
    networkThread.start()

    implicit val ec: ExecutionContext = client.clientContext

    def randomContent: Array[Byte] =
      val arr = new Array[Byte](16)
      val r = UUID.randomUUID()
      val bb = ByteBuffer.wrap(arr)
      bb.order(ByteOrder.BIG_ENDIAN)
      bb.putLong(r.getMostSignificantBits)
      bb.putLong(r.getLeastSignificantBits)
      arr

    def allocObject(ovalue: Option[ValueState],
                    kvos: KeyValueObjectState,
                    alloc: ObjectAllocator): Future[DataObjectPointer] = ovalue match
      case Some(v) =>
        println("------------- Using existing object -------------")
        Future.successful(ObjectPointer(v.value.bytes).asInstanceOf[DataObjectPointer])
      case None =>
        println("------------- Allocating new Object ------------")
        val tx = client.newTransaction()
        val key = Key(100)
        for
          ptr <- alloc.allocateDataObject(ObjectRevisionGuard(kvos.pointer, kvos.revision),
            randomContent)(tx)
          _ = tx.update(kvos.pointer, None, None, DoesNotExist(key) :: Nil, Insert(key, ptr.toArray) :: Nil)
          _ <- tx.commit()
        yield
            ptr


    println("------------ Reading Nucleus ---------------")
    for
      kvos <- client.read(nucleus)
      _ = println("------------ Getting Storage Pool---------------")
      pool <- client.getStoragePool(kvos.pointer.poolId)
      alloc = pool.createAllocater(Replication(3,2))
      _ = println("------------ Allocating Data Object ---------------")
      key = Key(100)
      dptr <- allocObject(kvos.contents.get(key), kvos, alloc)

      _ = println("------------ Reading Object r---------------")
      os <- client.read(dptr)

      tx = client.newTransaction()
      _ = tx.overwrite(dptr, os.revision, DataBuffer(randomContent))
      _ = println("------------ Committing random update ---------------")
      _ <- tx.commit()
    yield
      ()
  }

  def amoeba_server(log4jConfigFile: File, cfg: ConfigFile.Config): Unit = {
    println(s"LOG4J CONFIG $log4jConfigFile")
    setLog4jConfigFile(log4jConfigFile)

    val (client, network, nucleus) = createAmoebaClient(cfg)

    val networkThread = new Thread {
      override def run(): Unit = {
        network.ioThread()
      }
    }
    networkThread.start()

    val f = initializeAmoeba(client, nucleus)

    val fs = Await.result(f, Duration(10000, MILLISECONDS))

    val exports = "/ 192.168.64.2(rw)\n"

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
      withOperationExecutor(new MDSOperationExecutor).
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


  def repair(client: AmoebaClient, storeManager: StoreManager): Unit =

    def deleteErrorEntry(node: KeyValueListNode, key: Key): Future[Unit] =
      val tx = client.newTransaction()
      val fdelete = node.delete(key)(tx)
      for
        _ <- tx.commit()
        _ <- fdelete
      yield ()

    def deleteErrorEntryByTimestamp(timestamp: HLCTimestamp,
                                    node: KeyValueListNode,
                                    key: Key): Future[Unit] =
      val tx = client.newTransaction()
      val fdeletePrep = node.delete(key,
        None,
        List(KeyValueUpdate.TimestampLessThan(key, timestamp)),
        (_,_) => Future.successful(()))(tx)
      for
        _ <- fdeletePrep
        _ <- tx.commit()
      yield ()

    def step2(pool: StoragePool, storeId: StoreId, ptr: ObjectPointer,
              node: KeyValueListNode, key: Key): Future[Unit] =
      val fos = ptr match
        case kp: KeyValueObjectPointer => client.read(kp)
        case dp: DataObjectPointer => client.read(dp)
      val frepair = Promise[Unit]()
      for
        os <- fos
        _ = storeManager.repair(storeId, os, frepair)
        _ <- frepair.future
        _ <- deleteErrorEntryByTimestamp(os.timestamp, node, key)
      yield
        println(s"**** REPAIR Complete: ${ptr.id}")
        ()

    def step1(ovalue: Option[ValueState], pool: StoragePool, storeId: StoreId,
              node: KeyValueListNode, key: Key): Future[Unit] = ovalue match
      case None =>
        // No object found in the allocation tree. It must have been deleted. Remove error tree entry
        // TODO - Race condition here where an outstanding AllocationFinalizationAction may not have completed
        //        before we come along to do a repair. Very unlikely but still possible
        deleteErrorEntry(node, key)
      case Some(value) => step2(pool, storeId, ObjectPointer(value.value.bytes), node, key)

    def repairOne(pool: StoragePool, storeId: StoreId)(node: KeyValueListNode,
                                                       key: Key, value: ValueState): Future[Unit] =
      val bb = ByteBuffer.wrap(key.bytes)
      bb.order(ByteOrder.BIG_ENDIAN)
      bb.get() // storeIndex
      val msb = bb.getLong()
      val lsb = bb.getLong()
      val objectId = ObjectId(new UUID(msb, lsb))
      println(s"**** REPAIRING Object: ${objectId}")
      for
        ovalue <- pool.allocationTree.get(Key(objectId.toBytes))
        _ <- step1(ovalue, pool, storeId, node, key)
      yield
        ()

    println(s"*** Beginning Repair Process ***")
    storeManager.getStoreIds.foreach: storeId =>
      val min = Array[Byte](1)
      val max = Array[Byte](1)
      min(0) = storeId.poolIndex
      max(0) = (storeId.poolIndex + 1).toByte
      for
        pool <- client.getStoragePool(storeId.poolId)
        _ <- pool.errorTree.foreachInRange(Key(min), Key(max), repairOne(pool, storeId))
      yield
        println(s"*** Repair Process Complete ***")
        Future {
          Thread.sleep(30000)
          repair(client, storeManager)
        }


  def node(nodeName: String, cfg: ConfigFile.Config): Unit = {

    val sched = Executors.newScheduledThreadPool(3)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(sched)

    val node = cfg.nodes.getOrElse(nodeName, throw new ConfigError(s"Invalid node name $nodeName"))

    setLog4jConfigFile(node.log4jConfigFile)

    val simpleCrl = node.crl match {
      case b: ConfigFile.SimpleCRL => SimpleCRL.Factory(Paths.get(b.path), b.numStreams, b.fileSizeMb * 1024 * 1024)
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
      simpleCrl,
      txFinalizerFactory,
      SimpleTransactionDriver.factory(txRetryDelay, txRetryCap),
      txHeartbeatPeriod,
      stores
    ) with SimpleDriverRecoveryMixin

    networkBridge.onode = Some(storeManager)

    val networkThread = new Thread {
      override def run(): Unit = {
        network.ioThread()
      }
    }
    networkThread.start()
    storeManager.start()

    // Kickoff repair loop
    repair(client, storeManager)

    networkThread.join()
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
    }.toList.filter( backend =>
      // Create all stores but filter this list down to just the bootstrap-pool for Bootstrap process
      backend.storeId.poolId.uuid.getMostSignificantBits == 0 &&
      backend.storeId.poolId.uuid.getLeastSignificantBits == 0)

    cfg.nodes.values.foreach { n =>
      n.crl match {
        case s: ConfigFile.SimpleCRL => mkdirectory(Paths.get(s.path))
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
    sched.shutdownNow()
  }

  def rebuild(log4jConfigFile: File, storeName: String, cfg: ConfigFile.Config): Unit = {

    setLog4jConfigFile(log4jConfigFile)

    val (client, network, nucleus) = createAmoebaClient(cfg)

    val networkThread = new Thread {
      override def run(): Unit = {
        network.ioThread()
      }
    }
    networkThread.start()

    implicit val ec: ExecutionContext = client.clientContext

    val arr = storeName.split(":")
    val poolName = arr(0)
    val storeIndex = Integer.parseInt(arr(1))

    var store: Backend = null
    var poolId: PoolId = PoolId(new UUID(0,0))
    var storeId: StoreId = StoreId(poolId, 0.toByte)

    cfg.nodes.foreach: (_, node) =>
      node.stores.foreach: s =>
        if s.pool == poolName && storeIndex == s.store then
          poolId = PoolId(cfg.pools(s.pool).uuid)
          storeId = StoreId(poolId, s.store.asInstanceOf[Byte])
          s.backend match {
            case b: ConfigFile.RocksDB =>
              println(s"Rebuilding data store ${poolName}:${storeIndex}. Path ${b.path}")
              store = new RocksDBBackend(b.path, storeId, ec)
          }

    assert(store != null)

    def rebuildObject(node:KeyValueListNode, key: Key, value: ValueState): Future[Unit] =
      def getMetadata(os: ObjectState): (ObjectType.Value, Metadata) = os match
        case kvos: KeyValueObjectState =>
          (ObjectType.KeyValue, Metadata(kvos.revision, kvos.refcount, kvos.timestamp))
        case dos: DataObjectState =>
          (ObjectType.Data, Metadata(dos.revision, dos.refcount, dos.timestamp))
        case _: MetadataObjectState =>
          assert(false, "Unsupported object type!")

      val objectId = ObjectId(key.bytes)
      val ptr = ObjectPointer(value.value.bytes)

      println(f"Rebuilding object: $objectId")

      val storePointer = ptr.getStorePointer(storeId) match
        case None => return Future.successful(()) // This store doesn't hold data for this object.
        case Some(sp) => sp

      val fos = ptr match
        case p: KeyValueObjectPointer => client.read(p)
        case p: DataObjectPointer => client.read(p)

      for
        os <- fos
        (objectType, metadata) = getMetadata(os)
        localData = os.getRebuildDataForStore(storeId)
        _ = store.rebuildWrite(os.id, objectType, metadata, storePointer, localData.getOrElse(DataBuffer()))
      yield
        println(f"Rebuilt object ${os.id}")

    for
      pool <- client.getStoragePool(poolId)
      allocTree = pool.allocationTree
      _ <- allocTree.foreach(rebuildObject)
    yield
      store.rebuildFlush()
      println("**** Rebuild Complete ****")
      ()
  }

}
