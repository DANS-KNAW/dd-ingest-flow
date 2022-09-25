/*
 * Copyright (C) 2022 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.dd2d.dansbag

import better.files.File
import nl.knaw.dans.easy.dd2d.dansbag.InformationPackageType.InformationPackageType
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.{ DefaultFormats, Formats }
import org.json4s.native.Serialization
import scalaj.http.{ Http, MultiPart }

import java.net.URI
import scala.util.Try

class DansBagValidator(serviceUri: URI, pingUri: URI, connTimeoutMs: Int, readTimeoutMs: Int) extends DebugEnhancedLogging {
  def checkConnection(): Unit = {
    logger.debug(s"Checking if validator service can be reached at $pingUri")
    val result = Try {
      Http(s"$pingUri")
        .timeout(connTimeoutMs, readTimeoutMs)
        .method("GET")
        .header("Accept", "text/plain")
        .asString
    } map {
      case r if r.code == 200 =>
        if (r.body.trim != "pong")
          throw new RuntimeException("Validate DANS bag ping URL did not respond with 'pong'")
        logger.debug("OK: validator service is reachable.")
        ()
      case r => throw new RuntimeException(s"Connection to Validate DANS Bag Service could not be established. Service responded with ${r.statusLine}")
    }

    // Force exception if result is a Failure
    result.get
  }

  def validateBag(bagDir: File, informationPackageType: InformationPackageType, profileVersion: Int): Try[DansBagValidationResult] = {
    implicit val jsonFormats: Formats = DefaultFormats
    trace(bagDir)
    Try {
      val validationUri = serviceUri.resolve(s"validate")
      val command = Serialization.write(DansBagValidationCommand(bagLocation = bagDir.path.toString, packageType = informationPackageType.toString))
      logger.debug(s"Calling Dans Bag Validation Service with  command '$command'")
      Http(s"${ validationUri.toASCIIString }")
        .timeout(connTimeoutMs, readTimeoutMs)
        .postMulti(MultiPart(data = command, mime = "application/json", name = "command", filename = "command"))
        .asString
    } flatMap {
      case r if r.code == 200 =>
        DansBagValidationResult.fromJson(r.body)
      case r =>
        throw new RuntimeException(s"DANS Bag Validation failed (${ r.code }): ${ r.body }")
    }
  }
}
