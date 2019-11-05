package com.ibm.amoeba

import java.io.File

import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class FileBasedTests extends FunSuite with Matchers with BeforeAndAfter {
  var tdir:File = _
  var tdirMgr: TempDirManager = _

  before {
    tdirMgr = new TempDirManager
    tdir = tdirMgr.tdir
    preTest()
  }

  after {
    preTempDirDeletion()

    tdirMgr.delete()
  }

  def preTest(): Unit = {}
  def preTempDirDeletion(): Unit = ()
}
