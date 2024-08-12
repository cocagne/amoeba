package org.aspen_ddp.aspen.demo

import java.io.{File, StringReader}
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.Executors
import org.aspen_ddp.aspen.AmoebaError
import org.aspen_ddp.aspen.client.KeyValueObjectState.ValueState
import org.aspen_ddp.aspen.client.{AspenClient, DataObjectState, Host, HostId, KeyValueObjectState, MetadataObjectState, ObjectAllocator, ObjectState, StoragePool}
import org.aspen_ddp.aspen.client.internal.SimpleAspenClient
import org.aspen_ddp.aspen.client.internal.allocation.SinglePoolObjectAllocator
import org.aspen_ddp.aspen.client.tkvl.{KVObjectRootManager, KeyValueListNode, Root, SinglePoolNodeAllocator, TieredKeyValueList}
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp, Nucleus}
import org.aspen_ddp.aspen.common.ida.{ReedSolomon, Replication}
import org.aspen_ddp.aspen.common.network.{ClientId, ClientRequest, ClientResponse, TxMessage}
import org.aspen_ddp.aspen.common.objects.{ByteArrayKeyOrdering, DataObjectPointer, Insert, Key, KeyValueObjectPointer, LexicalKeyOrdering, Metadata, ObjectId, ObjectPointer, ObjectRevisionGuard, ObjectType, Value}
import org.aspen_ddp.aspen.common.pool.PoolId
import org.aspen_ddp.aspen.common.store.{StoreId, StorePointer}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.{DoesNotExist, KeyRequirement}
import org.aspen_ddp.aspen.common.util.{BackgroundTaskPool, YamlFormat, someOrThrow}
import org.aspen_ddp.aspen.common.ida.IDA
import org.aspen_ddp.aspen.amoebafs.FileSystem
import org.aspen_ddp.aspen.demo.network.{ZCnCBackend, ZCnCFrontend, ZMQNetwork, ZStoreTransferBackend}
import org.aspen_ddp.aspen.amoebafs.impl.simple.SimpleFileSystem
import org.aspen_ddp.aspen.amoebafs.nfs.AmoebaNFS
import org.aspen_ddp.aspen.server.cnc.TransferStore
import org.aspen_ddp.aspen.server.crl.simple.SimpleCRL
import org.aspen_ddp.aspen.server.{RegisteredTransactionFinalizerFactory, SimpleDriverRecoveryMixin, StoreManager}
import org.aspen_ddp.aspen.server.store.Bootstrap
import org.aspen_ddp.aspen.server.store.backend.{Backend, RocksDBBackend, RocksDBType}
import org.aspen_ddp.aspen.server.store.cache.SimpleLRUObjectCache
import org.aspen_ddp.aspen.server.transaction.SimpleTransactionDriver
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
                  nodeConfigFile:File=null,
                  bootstrapConfigFile:File=null,
                  log4jConfigFile: File=null,
                  nodeName:String="",
                  storeName:String="",
                  host:String="",
                  port:Int=0,
                  newPoolName: String="",
                  idaType: String="",
                  width:Int=0,
                  readThreshold:Int=0,
                  writeThreshold:Int=0,
                  hosts:List[String]=Nil)

  class ConfigError(msg: String) extends AmoebaError(msg)

  class NetworkBridge extends Logging {
    var oclient: Option[AspenClient] = None
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
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action( (x, c) => c.copy(bootstrapConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Bootstrap Config file does not exist: $x"))
        )

      cmd("debug").text("Runs debugging code").
        action((_, c) => c.copy(mode = "debug")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Bootstrap Config file does not exist: $x")),

          arg[File]("<log4j-config-file>").text("Log4j Configuration File").
            action( (x, c) => c.copy(log4jConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Log4j Config file does not exist: $x"))
        )

      cmd("node").text("Starts an Amoeba Storage Node").
        action( (_,c) => c.copy(mode="node")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action( (x, c) => c.copy(bootstrapConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Bootstrap Config file does not exist: $x")),

          //arg[String]("<node-name>").text("Storage Node Name").action((x,c) => c.copy(nodeName=x))
          arg[File]("<node-config-file>").text("Node Configuration File").
            action( (x, c) => c.copy(nodeConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Node Config file does not exist: $x"))
        )

      cmd("nfs").text("Launches a Amoeba NFS server").
        action( (_,c) => c.copy(mode="amoeba")).
        children(
          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action( (x, c) => c.copy(bootstrapConfigFile=x)).
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

          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action( (x, c) => c.copy(bootstrapConfigFile=x)).
            validate( x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<store-identifier>").text("Data Store Identifier. Format is \"pool-uuid:storeNumber\"").
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

      cmd("new-pool").text("Creates a new storage pool").
        action((_, c) => c.copy(mode = "new-pool")).
        children(
          arg[File]("<log4j-config-file>").text("Log4j Configuration File").
            action((x, c) => c.copy(log4jConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Log4j Config file does not exist: $x")),

          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<new-pool-name>").text("Name of the new Pool").
            action((x, c) => c.copy(newPoolName = x)),

          arg[String]("<ida-type>").text("IDA type. Must be Replication or Reed-Solomon").
            action((x, c) => c.copy(idaType = x.toLowerCase())).
            validate { x =>
              val xl = x.toLowerCase
              if xl == "replication" || xl == "reed-solomon" then
                success
              else
                failure("IDA type must be Replication or Reed-Solomon")
            },

          arg[Int]("<width>").text("Number of hosts holding slices/replicas").
            action((x, c) => c.copy(width = x)),

          arg[Int]("<read-threshold>").text("Minimum number of slices/replicas that must be read to reconstruct an object").
            action((x, c) => c.copy(readThreshold = x)),

          arg[Int]("<write-threshold>").text("Minimum number of slices/replicas that must be written to successfully write an object").
            action((x, c) => c.copy(writeThreshold = x)),

          arg[Seq[String]]("<hosts>").text("Comma-separated list of host names to host the object slice/replicas").
            action((x, c) => c.copy(hosts = x.toList)),
        )

      cmd("transfer-store").text("Transfers a store to a new host").
        action((_, c) => c.copy(mode = "transfer-store")).
        children(
          arg[File]("<log4j-config-file>").text("Log4j Configuration File").
            action((x, c) => c.copy(log4jConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Log4j Config file does not exist: $x")),

          arg[File]("<bootstrap-config-file>").text("Bootstrap Configuration File").
            action((x, c) => c.copy(bootstrapConfigFile = x)).
            validate(x => if (x.exists()) success else failure(s"Config file does not exist: $x")),

          arg[String]("<store-identifier>").text("Data Store Identifier. Format is \"pool-uuid:storeNumber\"").
            action((x, c) => c.copy(storeName = x)).
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
            },
          arg[String]("<new-host>").text("Name of the host to receive the store").
            action((x, c) => c.copy(host = x)),
        )
      checkConfig( c => if (c.mode == "") failure("Invalid command") else success )
    }

    parser.parse(args, Args()) match {
      case Some(cfg) =>
        //
        try {
          println(s"Loading BootstrapConfig ${cfg.bootstrapConfigFile}")
          val bootstrapConfig = BootstrapConfig.loadBootstrapConfig(cfg.bootstrapConfigFile)
          println(s"Successful config: $cfg")
          //println(s"Config file: $config")
          cfg.mode match {
            case "bootstrap" => bootstrap(bootstrapConfig, Paths.get("local/bootstrap"))
            case "node" => node(bootstrapConfig, StorageNodeConfig.loadStorageNode(cfg.nodeConfigFile))
            case "amoeba" => amoeba_server(cfg.log4jConfigFile, bootstrapConfig)
            case "debug" => run_debug_code(cfg.log4jConfigFile, bootstrapConfig)
            case "rebuild" => rebuild(cfg.log4jConfigFile, cfg.storeName, bootstrapConfig)
            case "new-pool" => new_pool(cfg.log4jConfigFile, bootstrapConfig, cfg.newPoolName, cfg.idaType, cfg.width, cfg.readThreshold, cfg.writeThreshold, cfg.hosts)
            case "transfer-store" => transfer_store(cfg.log4jConfigFile, bootstrapConfig, cfg.storeName, cfg.host)
          }
        } catch {
          case e: YamlFormat.FormatError => println(s"Error loading config file: $e")
          case e: ConfigError => println(s"Error: $e")
        }
      case None =>
    }
  }

  def createNetwork(cfg:BootstrapConfig.Config,
                    storageNode: Option[(String, String, Int)],
                    oclientId: Option[ClientId]): (NetworkBridge, ZMQNetwork) = {
    val b = new NetworkBridge
    val nodes = cfg.nodes.map(ds => ds.name -> (ds.host, ds.dataPort)).toMap
    val stores = cfg.nodes.zipWithIndex.map { (node, index) =>
      StoreId(PoolId(new UUID(0,0)), index.toByte) -> node.name
    }.toMap

    val heartbeatPeriod = Duration(10, SECONDS)
    (b, new ZMQNetwork(oclientId, nodes, stores, storageNode, heartbeatPeriod,
      b.onClientResponseReceived,
      b.onClientRequestReceived,
      b.onTransactionMessageReceived))
  }

  def createAmoebaClient(cfg: BootstrapConfig.Config,
                         onnet: Option[(NetworkBridge, ZMQNetwork)]=None): (AspenClient, ZMQNetwork, KeyValueObjectPointer) = {

    val hosts = cfg.nodes.zipWithIndex.map { (node, index) =>
      HostId(new UUID(0, index)) -> Host(HostId(new UUID(0, index)), node.name, node.host, node.dataPort, node.cncPort, node.storeTransferPort)
    }.toMap

    val (networkBridge, nnet) = onnet.getOrElse(createNetwork(cfg, None, None))

    val txStatusCacheDuration = Duration(10, SECONDS)
    val initialReadDelay = Duration(10, SECONDS)
    val maxReadDelay = Duration(6, SECONDS)
    val txRetransmitDelay = Duration(1, SECONDS)
    val allocationRetransmitDelay = Duration(5, SECONDS)

    val sched = Executors.newScheduledThreadPool(3)
    val ec: ExecutionContext = ExecutionContext.fromExecutorService(sched)

    val nucleus = KeyValueObjectPointer(Nucleus.objectId, Nucleus.poolId, None,
      cfg.bootstrapIDA, (0 until cfg.bootstrapIDA.width).map(idx => StorePointer(idx.toByte, Array())).toArray)

    val ret = (new SimpleAspenClient(nnet.clientMessenger, nnet.clientId, ec, nucleus,
      txStatusCacheDuration,
      initialReadDelay,
      maxReadDelay,
      txRetransmitDelay,
      allocationRetransmitDelay,
      hosts),  nnet, nucleus)

    networkBridge.oclient = Some(ret._1)

    ret
  }

  def initializeAmoeba(client: AspenClient,
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
        client.getStoragePool(kvos.pointer.poolId).flatMap { opool =>
          val allocator = new SinglePoolObjectAllocator(client, opool.get, kvos.pointer.ida, None)
          SimpleFileSystem.bootstrap(client, guard, allocator, kvos.pointer, AmoebafsKey)
        }
    }

    client.read(nucleus).flatMap(loadFileSystem)
  }

  def OLD_run_debug_code(log4jConfigFile: File, cfg: BootstrapConfig.Config): Unit = {
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

  def run_debug_code(log4jConfigFile: File, cfg: BootstrapConfig.Config): Unit = {
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
      alloc = pool.get.createAllocator(Replication(3,2))
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

  def amoeba_server(log4jConfigFile: File, cfg: BootstrapConfig.Config): Unit = {
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


  def repair(client: AspenClient, storeManager: StoreManager): Unit =

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
        opool <- client.getStoragePool(storeId.poolId)
        _ <- opool.get.errorTree.foreachInRange(Key(min), Key(max), repairOne(opool.get, storeId))
      yield
        println(s"*** Repair Process Complete for Store ${storeId} ***")
        Future {
          Thread.sleep(30000)
          repair(client, storeManager)
        }


  def node(bootstrapCfg: BootstrapConfig.Config,
           nodeCfg: StorageNodeConfig.StorageNode): Unit = {

    val sched = Executors.newScheduledThreadPool(3)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(sched)

    setLog4jConfigFile(nodeCfg.log4jConfigFile)

    val simpleCrl = nodeCfg.crl match {
      case b: StorageNodeConfig.SimpleCRL =>
        val crlRoot = Paths.get(s"${nodeCfg.rootDir}/crl")
        if ! Files.exists(crlRoot) then
          mkdirectory(crlRoot)
        SimpleCRL.Factory(crlRoot, b.numStreams, b.fileSizeMb * 1024 * 1024)
    }

    val objectCacheFactory = () => new SimpleLRUObjectCache(100)

    val nodeEndpoint = Some(nodeCfg.name, nodeCfg.endpoint.host, nodeCfg.endpoint.dataPort)

    val (networkBridge, nnet) = createNetwork(bootstrapCfg, nodeEndpoint, None)

    val (client, network, _) = createAmoebaClient(bootstrapCfg, Some((networkBridge, nnet)))

    networkBridge.oclient = Some(client)

    val txFinalizerFactory = new RegisteredTransactionFinalizerFactory(client)
    val txHeartbeatPeriod = Duration(5, SECONDS)
    val txRetryDelay = Duration(100, MILLISECONDS) //
    val txRetryCap = Duration(3, SECONDS)
    //val allocHeartbeatPeriod   = Duration(3, SECONDS)
    //val allocTimeout           = Duration(4, SECONDS)
    //val allocStatusQueryPeriod = Duration(1, SECONDS)

    val nodeNet = nnet.serverMessenger

    val storeManager = new StoreManager(
      nodeCfg.rootDir,
      ec,
      objectCacheFactory,
      nodeNet,
      new BackgroundTaskPool,
      simpleCrl,
      txFinalizerFactory,
      SimpleTransactionDriver.factory(txRetryDelay, txRetryCap),
      new BackendStoreLoaderImpl,
      txHeartbeatPeriod,
    ) with SimpleDriverRecoveryMixin

    networkBridge.onode = Some(storeManager)

    val networkThread = new Thread {
      override def run(): Unit = {
        network.ioThread()
      }
    }
    networkThread.start()
    storeManager.start()

    val cncBackend = new ZCnCBackend(
      nnet,
      client,
      nodeCfg.rootDir.resolve("stores"),
      storeManager :: Nil,
      nodeCfg.endpoint.cncPort)

    client.getHost(nodeCfg.name).foreach:
      case None => throw new Exception(s"Invalid Host Name: ${nodeCfg.name}")
      case Some(host) =>
        val transferBackend = new ZStoreTransferBackend(
          nodeCfg.endpoint.storeTransferPort,
          network,
          host.hostId,
          client,
          storeManager)

    // Kickoff repair loop
    repair(client, storeManager)

    networkThread.join()
  }

  def mkdirectory(p: Path): Unit = {
    Files.createDirectories(p)
  }

  def bootstrap(cfg: BootstrapConfig.Config, storesDir: Path): Unit = {

    val sched = Executors.newScheduledThreadPool(1)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(sched)

    val bootstrapStores = cfg.nodes.zipWithIndex.map: (node, poolIndex) =>

      val dataStoreId = StoreId(PoolId(new UUID(0,0)), poolIndex.toByte)

      val storeRoot = storesDir.resolve(s"${dataStoreId.poolId.uuid}:$poolIndex")

      println(s"Creating data store $dataStoreId. Path $storeRoot")
      mkdirectory(storeRoot)
      Files.writeString(storeRoot.resolve("store_config.yaml"),
      s"""
          |pool-uuid: "00000000-0000-0000-0000-000000000000"
          |store-index: $poolIndex
          |backend:
          |  storage-engine: rocksdb
          |""".stripMargin)
      new RocksDBBackend(storeRoot, dataStoreId, ec)

    val nucleus = Bootstrap.initialize(cfg.bootstrapIDA,
      bootstrapStores,
      cfg.nodes.zipWithIndex.map((n, idx) => (n.name, new UUID(0, idx))))

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

  def rebuild(log4jConfigFile: File, storeName: String, cfg: BootstrapConfig.Config): Unit = {

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
    val poolUuid = UUID.fromString(arr(0))
    val storeIndex = Integer.parseInt(arr(1))

    var store: Backend = null
    var poolId: PoolId = PoolId(new UUID(0,0))
    var storeId: StoreId = StoreId(poolId, 0.toByte)

    cfg.nodes.zipWithIndex.foreach: (node, index) =>
      Path.of(s"local/${node.name}/stores").toFile.listFiles.toList.foreach: storeFn =>
        val cfg = StoreConfig.loadStore(storeFn.toPath.resolve("store_config.yaml").toFile)
        if poolUuid == cfg.poolUuid && storeIndex == cfg.index then
          poolId = PoolId(cfg.poolUuid)
          storeId = StoreId(poolId, cfg.index.toByte)
          cfg.backend match {
            case b: StoreConfig.RocksDB =>
              println(s"Rebuilding data store $poolUuid:$storeIndex. Path $storeFn")
              store = new RocksDBBackend(storeFn.toPath, storeId, ec)
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
      opool <- client.getStoragePool(poolId)
      allocTree = opool.get.allocationTree
      _ <- allocTree.foreach(rebuildObject)
    yield
      store.rebuildFlush()
      println("**** Rebuild Complete ****")
      ()
  }

  def new_pool(log4jConfigFile: File,
               bootstrapConfig: BootstrapConfig.Config,
               newPoolName: String,
               idaType: String,
               width: Int,
               readThreshold: Int,
               writeThreshold: Int,
               hosts: List[String]): Unit = {
    println(s"READ $readThreshold, WRITE $writeThreshold")
    require(hosts.length == width)
    require(width >= readThreshold && width >= writeThreshold)
    require(readThreshold <= writeThreshold)

    setLog4jConfigFile(log4jConfigFile)

    val (client, network, nucleus) = createAmoebaClient(bootstrapConfig)

    val networkThread = new Thread {
      override def run(): Unit = {
        network.ioThread()
      }
    }
    networkThread.start()

    implicit val ec: ExecutionContext = client.clientContext

    val ida: IDA = idaType match
      case "replication" => Replication(width, writeThreshold)
      case "reed-solomon" => ReedSolomon(width, readThreshold, writeThreshold)
      case _ => throw new Exception(s"Invalid IDA type: $idaType")

    def getHost(name: String): Future[Host] =
      client.getHost(name).map:
        case None => throw new Exception(f"Host name not found: $name")
        case Some(host) => host

    for
      hlist <- Future.sequence(hosts.map(getHost))
      frontends = hlist.map(host => new ZCnCFrontend(network, host))
      sp <- client.newStoragePool(newPoolName, frontends, ida, RocksDBType())
    yield
      println("******************************************")
      println(f"* New Pool Created: ${sp.poolId}")
      println("******************************************")
  }

  def transfer_store(log4jConfigFile: File,
                     bootstrapConfig: BootstrapConfig.Config,
                     storeName: String,
                     hostName: String): Unit = {

    setLog4jConfigFile(log4jConfigFile)

    val (client, network, nucleus) = createAmoebaClient(bootstrapConfig)

    val networkThread = new Thread {
      override def run(): Unit = {
        network.ioThread()
      }
    }
    networkThread.start()

    implicit val ec: ExecutionContext = client.clientContext

    val storeId = StoreId(storeName)

    for
      newHost <- someOrThrow(client.getHost(hostName), new Exception(f"Host name not found: $hostName"))
      sp <- someOrThrow(client.getStoragePool(storeId.poolId), new Exception(f"StoragePool not found ${storeId.poolId}"))
      curHostId = sp.storeHosts(storeId.poolIndex)
      currentHost <- someOrThrow(client.getHost(curHostId), new Exception(f"Host name not found: $curHostId"))

      zfrontend = new ZCnCFrontend(network, currentHost)
      _ <- zfrontend.send(TransferStore(storeId, newHost.hostId))
    yield
      println(f"Store Transfer Initiated: Store: ${storeName} From: ${currentHost.name} To: ${hostName}")
  }

}
