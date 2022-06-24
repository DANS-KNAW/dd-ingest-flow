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
import nl.knaw.dans.lib.scaladv.model.RoleAssignment
import nl.knaw.dans.lib.scaladv.model.dataset.Dataset
import nl.knaw.dans.lib.scaladv.{ Version, serializeAsJson }
import org.json4s.native.Serialization

import java.net.URI
import java.util.regex.Pattern
import java.util.{ Date, Optional }
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
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
                     dataverseClient: DataverseClient,
                     optMigrationInfoService: Option[MigrationInfo]) extends DatasetEditor(dataverseClient, optFileExclusionPattern, zipFileHandler) with DebugEnhancedLogging {
  trace(deposit)

  override def performEdit(): Try[PersistentId] = {
    {
      val javaDataverseApi = dataverseClient.dataverse("root")
      for {
        jsonString <- serializeAsJson(dataverseDataset, logger.underlying.isDebugEnabled)
        // autoPublish is false, because it seems there is a bug with it in Dataverse (most of the time?)
        response <- Try(if (isMigration)
                          javaDataverseApi.importDataset(jsonString, Optional.of(s"doi:${ deposit.doi }"), false)
                        else javaDataverseApi.createDataset(jsonString)
        )
        persistentId <- Try(response.getData).map(_.getPersistentId)
      } yield persistentId
    } match {
      case Failure(e) => Failure(FailedDepositException(deposit, "Could not import/create dataset", e))
      case Success(persistentId) => {
        for {
          licenseAsJson <- licenseAsJson(supportedLicenses)(variantToLicense)(deposit)
          javaDatasetApi <- Try(dataverseClient.dataset(persistentId))
          _ <- Try(javaDatasetApi.updateMetadataFromJsonLd(licenseAsJson, true))
          _ <- Try(javaDatasetApi.awaitUnlock())
          pathToFileInfo <- getPathToFileInfo(deposit)
          prestagedFiles <- optMigrationInfoService.map(_.getPrestagedDataFilesFor(s"doi:${ deposit.doi }", 1)).getOrElse(Success(Set.empty[BasicFileMeta]))
          databaseIdsToFileInfo <- addFiles(persistentId, pathToFileInfo.values.toList, prestagedFiles)
          _ <- updateFileMetadata(databaseIdsToFileInfo.mapValues(_.metadata))
          _ <- Try(javaDatasetApi.awaitUnlock())
          _ <- configureEnableAccessRequests(deposit, persistentId, canEnable = true)
          _ <- Try(javaDatasetApi.awaitUnlock())
          _ = debug(s"Assigning role $depositorRole to ${ deposit.depositorUserId }")
          scalaRoleAssignment = RoleAssignment(s"@${ deposit.depositorUserId }", depositorRole)
          jsonRoleAssignment = Serialization.write(scalaRoleAssignment)
          _ <- Try(javaDatasetApi.assignRole(jsonRoleAssignment))
          _ <- Try(javaDatasetApi.awaitUnlock())
          dateAvailable <- deposit.getDateAvailable
          _ <- embargoFiles(persistentId, dateAvailable)
        } yield persistentId
      }.doIfFailure {
        case NonFatal(e) =>
          logger.error("Dataset creation failed, deleting draft", e)
          deleteDraftIfExists(persistentId)
      }
    }
  }

  private def embargoFiles(persistentId: PersistentId, dateAvailable: Date): Try[Unit] =
    if (!isEmbargo(dateAvailable)) {
      logger.debug(s"Date available in the past, no embargo: $dateAvailable")
      Success(())
    }
    else {
      logger.info(s"Putting embargo on files until: $dateAvailable")
      for {
        response <- Try(dataverseClient.dataset(persistentId).getFiles(Version.LATEST.toString()).getData)
        ids = response.filter(f => "easy-migration" != f.getDirectoryLabel)
          .map(f => f.getDataFile.getId).toList
        _ <- embargoFiles(persistentId, dateAvailable, ids)
        _ <- Try(dataverseClient.dataset(persistentId).awaitUnlock())
      } yield ()
    }
}
