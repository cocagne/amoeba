package com.ibm.amoeba

import java.io.File

class TempDirManager {

  private [this] val tfile = File.createTempFile("scalatest", "UnitTestTempDir")
  tfile.delete()

  val tdir:File = new File(tfile.toString)
  tdir.mkdir()


  def delete(): Unit = {

    def cleanup(f:File): Unit = {
      if (f.isFile) {
        f.delete()
      }
      else {
        f.listFiles().foreach( cleanup )
        f.delete()
      }
    }

    cleanup(tdir)
  }
}