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
import nl.knaw.dans.easy.dd2d.dansbag.{ DansBagValidator, InformationPackageType }
import nl.knaw.dans.easy.dd2d.dansbag.InformationPackageType.InformationPackageType
import nl.knaw.dans.easy.dd2d.mapping.Amd
import nl.knaw.dans.easy.dd2d.migrationinfo.MigrationInfo
import nl.knaw.dans.lib.dataverse.DataverseClient
import nl.knaw.dans.lib.scaladv.DataverseInstance
import nl.knaw.dans.lib.scaladv.model.dataset.Dataset

import java.net.URI
import java.util.regex.Pattern
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }
import scala.xml.{ Elem, Node }

class DepositMigrationTask(deposit: Deposit,
                           optFileExclusionPattern: Option[Pattern],
                           zipFileHandler: ZipFileHandler,
                           depositorRole: String,
                           deduplicate: Boolean,
                           activeMetadataBlocks: List[String],
                           optDansBagValidator: Option[DansBagValidator],
                           dataverseInstance: DataverseInstance,
                           dataverseClient: DataverseClient,
                           migrationInfo: Option[MigrationInfo],
                           publishAwaitUnlockMaxNumberOfRetries: Int,
                           publishAwaitUnlockMillisecondsBetweenRetries: Int,
                           narcisClassification: Elem,
                           iso1ToDataverseLanguage: Map[String, String],
                           iso2ToDataverseLanguage: Map[String, String],
                           variantToLicense: Map[String, String],
                           supportedLicenses: List[URI],
                           repordIdToTerm: Map[String, String],
                           outboxDir: File)
  extends DepositIngestTask(deposit,
    optFileExclusionPattern,
    zipFileHandler,
    depositorRole,
    deduplicate,
    activeMetadataBlocks,
    optDansBagValidator,
    dataverseInstance,
    dataverseClient,
    migrationInfo: Option[MigrationInfo],
    publishAwaitUnlockMaxNumberOfRetries,
    publishAwaitUnlockMillisecondsBetweenRetries,
    narcisClassification,
    iso1ToDataverseLanguage,
    iso2ToDataverseLanguage,
    variantToLicense,
    supportedLicenses,
    repordIdToTerm,
    outboxDir) {
  override protected val informationPackageType: InformationPackageType = InformationPackageType.AIP

  override protected def checkDepositType(): Try[Unit] = {
    for {
      _ <- if (deposit.doi.isEmpty) Failure(new IllegalArgumentException("Deposit for migrated dataset MUST have deposit property identifier.doi set"))
           else Success(())
      _ <- deposit.vaultMetadata.checkMinimumFieldsForImport()
    } yield ()
  }

  override def newDatasetUpdater(dataverseDataset: Dataset): DatasetUpdater = {
    new DatasetUpdater(deposit, optFileExclusionPattern, zipFileHandler, isMigration = true, dataverseDataset.datasetVersion.metadataBlocks, variantToLicense, supportedLicenses, dataverseInstance, dataverseClient, migrationInfo)
  }

  override def newDatasetCreator(dataverseDataset: Dataset, depositorRole: String): DatasetCreator = {
    new DatasetCreator(deposit, optFileExclusionPattern, zipFileHandler, depositorRole, isMigration = true, dataverseDataset, variantToLicense, supportedLicenses, dataverseClient, migrationInfo)
  }

  override protected def checkPersonalDataPresent(optAgreements: Option[Node]): Try[Unit] = {
    if (optAgreements.isEmpty) Failure(RejectedDepositException(deposit, "Migration deposit MUST have an agreements.xml"))
    else Success(())
  }

  override protected def getDateOfDeposit: Try[Option[String]] = {
    for {
      optAmd <- deposit.tryOptAmd
      optDate = optAmd.flatMap(Amd toDateOfDeposit)
    } yield optDate
  }

  override protected def publishDataset(persistentId: String): Try[Unit] = {
    trace(persistentId)
    for {
      optAmd <- deposit.tryOptAmd
      amd = optAmd.getOrElse(throw new Exception(s"no AMD found for $persistentId"))
      optPublicationDate <- getJsonLdPublicationdate(amd)
      publicationDate = optPublicationDate.getOrElse(throw new IllegalArgumentException(s"no publication date found in AMD for $persistentId"))
      _ <- dataverseInstance.dataset(persistentId).releaseMigrated(publicationDate)
      _ <- dataverseInstance.dataset(persistentId).awaitUnlock(
        maxNumberOfRetries = publishAwaitUnlockMaxNumberOfRetries,
        waitTimeInMilliseconds = publishAwaitUnlockMillisecondsBetweenRetries)
    } yield ()
  }

  private def getJsonLdPublicationdate(amd: Node): Try[Option[String]] = Try {
    Amd.toPublicationDate(amd)
      .map(d => s"""{"http://schema.org/datePublished": "$d"}""")
  }

  override protected def postPublication(persistentId: String): Try[Unit] = {
    trace(persistentId)
    Success(())
  }
}
