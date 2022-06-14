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

import nl.knaw.dans.easy.dd2d.migrationinfo.{ BasicFileMeta, MigrationInfo }
import nl.knaw.dans.lib.dataverse.DataverseClient
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.lib.scaladv.DataverseInstance
import nl.knaw.dans.lib.scaladv.model.RoleAssignment
import nl.knaw.dans.lib.scaladv.model.dataset.Dataset
import org.json4s.native.Serialization
import org.json4s.{ DefaultFormats, Formats }

import java.net.URI
import java.util.Date
import java.util.regex.Pattern
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

class DatasetCreator(deposit: Deposit,
                     optFileExclusionPattern: Option[Pattern],
                     zipFileHandler: ZipFileHandler,
                     depositorRole: String,
                     isMigration: Boolean = false,
                     dataverseDataset: Dataset,
                     variantToLicense: Map[String, String],
                     supportedLicenses: List[URI],
                     dataverseInstance: DataverseInstance,
                     dataverseClient: DataverseClient,
                     optMigrationInfoService: Option[MigrationInfo]) extends DatasetEditor(dataverseInstance, optFileExclusionPattern, zipFileHandler) with DebugEnhancedLogging {
  trace(deposit)
  private implicit val jsonFormats: Formats = DefaultFormats

  override def performEdit(): Try[PersistentId] = {
    {
      val dataverseApi = dataverseInstance.dataverse("root")
      for {
        // autoPublish is false, because it seems there is a bug with it in Dataverse (most of the time?)
        response <- if (isMigration)
                      dataverseApi.importDataset(dataverseDataset, Some(s"doi:${ deposit.doi }"), autoPublish = false)
                    else dataverseApi.createDataset(dataverseDataset)
        persistentId <- response.data.map(_.persistentId)
      } yield persistentId
    } match {
      case Failure(e) => Failure(FailedDepositException(deposit, "Could not import/create dataset", e))
      case Success(persistentId) => {
        for {
          licenseAsJson <- licenseAsJson(supportedLicenses)(variantToLicense)(deposit)
          _ <- Try(dataverseClient.dataset(persistentId).updateMetadataFromJsonLd(licenseAsJson, true))
          _ <- Try(dataverseClient.dataset(persistentId).awaitUnlock())
          pathToFileInfo <- getPathToFileInfo(deposit)
          prestagedFiles <- optMigrationInfoService.map(_.getPrestagedDataFilesFor(s"doi:${ deposit.doi }", 1)).getOrElse(Success(Set.empty[BasicFileMeta]))
          databaseIdsToFileInfo <- addFiles(persistentId, pathToFileInfo.values.toList, prestagedFiles)
          _ <- updateFileMetadata(databaseIdsToFileInfo.mapValues(_.metadata))
          _ <- Try(dataverseClient.dataset(persistentId).awaitUnlock())
          _ <- configureEnableAccessRequests(deposit, persistentId, canEnable = true)
          _ <- Try(dataverseClient.dataset(persistentId).awaitUnlock())
          _ = debug(s"Assigning role $depositorRole to ${ deposit.depositorUserId }")
          scalaRoleAssignment = RoleAssignment(s"@${ deposit.depositorUserId }", depositorRole)
          jsonRoleAssignment = Serialization.write(scalaRoleAssignment)
          _ <- Try(dataverseClient.dataset(persistentId).assignRole(jsonRoleAssignment))
          _ <- Try(dataverseClient.dataset(persistentId).awaitUnlock())
          dateAvailable <- deposit.getDateAvailable
          _ <- if (isEmbargo(dateAvailable)) embargoFiles(persistentId, dateAvailable)
               else {
                 logger.debug(s"Date available in the past, no embargo: $dateAvailable")
                 Success(())
               }
        } yield persistentId
      }.doIfFailure {
        case NonFatal(e) =>
          logger.error("Dataset creation failed, deleting draft", e)
          deleteDraftIfExists(persistentId)
      }
    }
  }

  private def embargoFiles(persistentId: PersistentId, dateAvailable: Date): Try[Unit] = {
    logger.info(s"Putting embargo on files until: $dateAvailable")
    for {
      files <- getFilesToEmbargo(persistentId)
      _ <- embargoFiles(persistentId, dateAvailable, files.map(_.dataFile.get.id))
      _ <- dataverseInstance.dataset(persistentId).awaitUnlock()
    } yield ()
  }
}
