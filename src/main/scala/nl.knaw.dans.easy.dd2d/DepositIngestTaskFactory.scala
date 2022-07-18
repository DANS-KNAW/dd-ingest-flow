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
package nl.knaw.dans.easy.dd2d

import better.files.File
import nl.knaw.dans.easy.dd2d.dansbag.DansBagValidator
import nl.knaw.dans.easy.dd2d.migrationinfo.MigrationInfo
import nl.knaw.dans.lib.dataverse.DataverseClient
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.csv.{ CSVFormat, CSVParser }
import org.apache.commons.io.FileUtils

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern
import scala.collection.JavaConverters.{ asScalaBufferConverter, asScalaIteratorConverter }
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.Try
import scala.xml.{ Elem, XML }

/**
 * Factory for creating ingest tasks.
 *
 * @param isMigrated                                   is this a migrated dataset?
 * @param activeMetadataBlocks                         the metadata blocks enabled in the target dataverse
 * @param optDansBagValidator                          interface to the easy-validate-dans-bag service
 * @param migrationInfo                                optional interface to a migration info service
 * @param publishAwaitUnlockMaxNumberOfRetries         maximum number of times to poll for unlock after publish is called after ingest of the deposit
 * @param publishAwaitUnlockMillisecondsBetweenRetries number of milliseconds to wait between retries of unlock polling after publish
 * @param narcisClassification                         root element of the NARCIS SKOS file
 * @param iso2ToDataverseLanguage                      mapping of ISO639-2 to Dataverse language term
 * @param reportIdToTerm                               mapping of ABR report ID to term
 */
class DepositIngestTaskFactory(isMigrated: Boolean = false,
                               optFileExclusionPattern: Option[Pattern],
                               zipFileHandler: ZipFileHandler,
                               depositorRole: String,
                               deduplicateService: Boolean,
                               deduplicateImport: Boolean,
                               activeMetadataBlocks: List[String],
                               optDansBagValidator: Option[DansBagValidator],
                               dataverseClient: DataverseClient,
                               migrationInfo: Option[MigrationInfo],
                               publishAwaitUnlockMaxNumberOfRetries: Int,
                               publishAwaitUnlockMillisecondsBetweenRetries: Int,
                               narcisClassification: Elem,
                               iso1ToDataverseLanguage: Map[String, String],
                               iso2ToDataverseLanguage: Map[String, String],
                               variantToLicense: Map[String, String],
                               supportedLicenses: List[URI],

                               reportIdToTerm: Map[String, String]) {

  def createDepositIngestTask(deposit: Deposit, outboxDir: File): DepositIngestTask = {
    if (isMigrated)
      new DepositMigrationTask(deposit,
        optFileExclusionPattern,
        zipFileHandler,
        depositorRole,
        deduplicateImport,
        activeMetadataBlocks,
        optDansBagValidator,
        dataverseClient,
        publishAwaitUnlockMaxNumberOfRetries,
        publishAwaitUnlockMillisecondsBetweenRetries,
        narcisClassification,
        iso1ToDataverseLanguage,
        iso2ToDataverseLanguage,
        variantToLicense,
        supportedLicenses,
        reportIdToTerm,
        outboxDir)
    else
      DepositIngestTask(
        deposit,
        optFileExclusionPattern,
        zipFileHandler,
        depositorRole,
        deduplicateService,
        activeMetadataBlocks,
        optDansBagValidator,
        dataverseClient,
        publishAwaitUnlockMaxNumberOfRetries,
        publishAwaitUnlockMillisecondsBetweenRetries,
        narcisClassification,
        iso1ToDataverseLanguage,
        iso2ToDataverseLanguage,
        variantToLicense,
        supportedLicenses,
        reportIdToTerm,
        outboxDir: File)
  }
}

object DepositIngestTaskFactory extends DebugEnhancedLogging {
  def getActiveMetadataBlocks(dataverseClient: DataverseClient): Try[List[String]] = {
    for {
      result <- Try(dataverseClient.dataverse("root").listMetadataBlocks())
      _ = logger.trace(result.getEnvelopeAsString)
      blocks <- Try(result.getData)
    } yield blocks.map(_.getName).toList
  }

  def readXml(file: java.io.File): Elem = {
    XML.loadFile(file)
  }

  def loadCsvToMap(csvFile: File, keyColumn: String, valueColumn: String): Try[Map[String, String]] = {
    import resource.managed

    def csvParse(csvParser: CSVParser): Map[String, String] = {
      csvParser.iterator().asScala
        .map { r => (r.get(keyColumn), r.get(valueColumn)) }.toMap
    }

    managed(CSVParser.parse(
      csvFile.toJava,
      StandardCharsets.UTF_8,
      CSVFormat.RFC4180.withFirstRecordAsHeader())).map(csvParse).tried
  }

  def loadTxtToUriList(txtFile: File): Try[List[URI]] = Try {
    FileUtils.readLines(txtFile.toJava, StandardCharsets.UTF_8).asScala.toList.map(new URI(_))
  }

  def appendSlash(uri: URI): URI = {
    if (uri.getPath.endsWith("/")) uri
    else new URI(uri + "/")  }
}