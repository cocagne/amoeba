
/* SETUP
 * 
 * This project depends on Flatbuffers which, unfortunately, must be manually installed.
 * To do so, download the flatbuffers source code, run "cmake ." from within the root
 * directory of the project, and then "make install" and "mvn install". This will
 * install the flatc utility as /usr/local/bin/flatc and create the flatbuffers jar file
 * in <flatbuffer_root>/target. Copy the jar file to the 'lib' folder of this project and
 * you should be good to go.
 *
 * Scala IDE project files can be generated with 'sbt eclipse'
 * 
 */

import scala.sys.process._

lazy val root = (project in file(".")).
  settings(
    name         := "amoeba",
    version      := "0.1",
    scalaVersion := "2.12.10",
    organization := "com.ibm",
      
    scalacOptions ++= Seq("-feature", "-deprecation"),

    resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/",
    
    resolvers += "dCache Repository" at "https://download.dcache.org/nexus/content/repositories/releases",
    resolvers += "Oracle Repository" at "http://download.oracle.com/maven",

    libraryDependencies ++= Seq(
      "org.scalatest"                    %% "scalatest"               % "3.0.8" % "test",
      "com.github.blemale"               %% "scaffeine"               % "3.1.0" % "compile",
      "org.rocksdb"                      %  "rocksdbjni"              % "6.3.6",
      "com.github.scopt"                 %% "scopt"                   % "4.0.0-RC2",
      "io.netty"                         %  "netty-all"               % "4.1.43.Final",
      "org.apache.logging.log4j"         %  "log4j-api"               % "2.12.1",
      "org.apache.logging.log4j"         %  "log4j-core"              % "2.12.1",
      "org.apache.logging.log4j"         %% "log4j-api-scala"         % "11.0",
      "org.slf4j"                        %  "slf4j-log4j12"           % "1.8.0-beta2",
      "com.lmax"                         %  "disruptor"               % "3.3.7",
      "org.dcache"                       %  "nfs4j-core"              % "0.19.0",
      "org.yaml"                         %  "snakeyaml"               % "1.25",
    )
  )
  
testOptions  in Test += Tests.Argument(TestFrameworks.ScalaTest, "-W", "10", "5")

//parallelExecution in Test := false


sourceGenerators in Compile += Def.task {
  val base = (sourceManaged in Compile).value

  // Network Protocol
  val net_out_dir = (sourceManaged in Compile).value / "com" / "ibm" / "amoeba" / "common" / "network" / "protocol"

  val net_schema = file("schema") / "network_protocol.fbs"

  val net_generate = !net_out_dir.exists() || net_out_dir.listFiles().exists( f => net_schema.lastModified() > f.lastModified() )
  
  //println(s"Dir exists $net_out_dir, ${net_out_dir.exists()}")

  //net_out_dir.listFiles().foreach( f => println(s"$f : ${net_schema.lastModified() > f.lastModified()}") )

  if (net_generate) {
    println(s"Generating Network Protocol Source Files")
    val stdout:Int = s"flatc --java -o $base schema/network_protocol.fbs".!
    println(s"Result: $stdout")
  }
  
  net_out_dir.listFiles().toSeq
}.taskValue
