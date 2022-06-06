package nl.knaw.dans.easy.dd2d

import nl.knaw.dans.lib.util.DataverseClientFactory

import java.net.URI

class MP extends TestSupportFixture {
  val dc = {
    val dcf = new DataverseClientFactory
    dcf.setBaseUrl(new URI("http://dar.dans.knaw.nl:8080"))
    dcf.setApiKey("09286b45-08ee-43bd-8867-057788afd7bd")
    dcf.build()
  }

  "proof of concept" should "" in {
    val x = dc.admin()
    // TODO causes java.lang.NoSuchMethodError JDK issue?
    //  https://www.michaelpollmeier.com/2014/10/12/calling-java-8-functions-from-scala
    //  https://github.com/scala/scala-java8-compat
    //  https://alvinalexander.com/source-code/scala-java-lang-nosuchmethoderror-compiler-message/
    // val newResponse = adminApi.listSingleUser(deposit.depositorUserId)
  }
}
