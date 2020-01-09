package com.ibm.amoeba

import com.ibm.amoeba.client.KeyValueObjectState
import com.ibm.amoeba.common.objects.{Insert, Key, Value}
import com.ibm.amoeba.common.transaction.KeyValueUpdate

import scala.concurrent.Future

class BasicIntegrationSuite extends IntegrationTestSuite {

  test("Read empty nucleus") {
    client.read(nucleus).map( kvos => kvos.contents.isEmpty should be (true) )
  }

  test("Insert key value pair into nucleus") {
    val key = Key(Array[Byte](1))
    val value = Value(Array[Byte](2))

    def update(kvos: KeyValueObjectState): Future[Unit] = {
      val tx = client.newTransaction()
      tx.update(nucleus,
        Some(kvos.revision),
        List(KeyValueUpdate.DoesNotExist(key)),
        List(Insert(key, value.bytes)))

      tx.commit().map(_=>())
    }

    for {
      ikvos <- client.read(nucleus)
      _ <- update(ikvos)
      kvos <- client.read(nucleus)
    } yield {
      kvos.contents.isEmpty should be (false)
      kvos.contents.contains(key) should be (true)
      kvos.contents(key).value.bytes.length should be (1)
      kvos.contents(key).value.bytes(0) should be (2)
    }

  }
}
