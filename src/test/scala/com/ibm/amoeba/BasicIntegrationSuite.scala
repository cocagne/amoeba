package com.ibm.amoeba

import scala.concurrent.Future

class BasicIntegrationSuite extends IntegrationTestSuite {

  test("Read empty nucleus") {
    client.read(nucleus).map( kvos => kvos.contents.isEmpty should be (true) )
  }
}
