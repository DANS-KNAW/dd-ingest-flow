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

import nl.knaw.dans.easy.dd2d.mapping.{ AccessRights, License }
import nl.knaw.dans.ingest.core.legacy.MetadataObjectMapper
import nl.knaw.dans.lib.dataverse.DataverseClient
import nl.knaw.dans.lib.dataverse.model.dataset.Embargo
import nl.knaw.dans.lib.dataverse.model.file.FileMeta
import nl.knaw.dans.lib.error.{ TraversableTryExtensions, TryExtensions }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.native.Serialization
import org.json4s.{ DefaultFormats, Formats }

import java.net.URI
import java.nio.file.{ Path, Paths }
import java.util.regex.Pattern
import java.util.{ Date, Optional }
import scala.util.{ Failure, Success, Try }

/**
 * Object that edits a dataset, a new draft.
 */
abstract class DatasetEditor(dataverseClient: DataverseClient, optFileExclusionPattern: Option[Pattern], zipFileHandler: ZipFileHandler) extends DebugEnhancedLogging {
  type PersistentId = String
  type DatasetId = Int
  implicit val jsonFormats: Formats = DefaultFormats

  /**
   * Performs the task.
   *
   * @return the persistentId of the dataset created or modified
   */
  def performEdit(): Try[PersistentId]

  protected def addFiles(persistentId: String, files: List[FileInfo]): Try[Map[Int, FileInfo]] = Try {
    trace(persistentId, files)
    files.map { f =>
      debug(s"Adding file, directoryLabel = ${ f.metadata.getDirectoryLabel }, label = ${ f.metadata.getLabel }")
      val id = addFile(persistentId, f).unsafeGetOrThrow
      dataverseClient.dataset(persistentId).awaitUnlock()
      id -> f
    }.toMap
  }

  private def addFile(doi: String, fileInfo: FileInfo): Try[Int] = {
    val result = for {
      r <- {
        debug(s"Uploading file: $fileInfo")
        val optWrappedZip = zipFileHandler.wrapIfZipFile(fileInfo.file)
        val r = Try(dataverseClient.dataset(doi).addFileItem(
          Optional.of(optWrappedZip.getOrElse(fileInfo.file).toJava), // TODO what about closing files?
          Optional.of(Serialization.write(fileInfo.metadata))
        ))
        optWrappedZip.foreach(_.delete(swallowIOExceptions = true))
        r
      }
      files <- Try(r.getData)
      triedId = Try(files.getFiles.get(0).getDataFile.getId)
      _ <- Try(dataverseClient.dataset(doi).awaitUnlock())
    } yield triedId
    debug(s"Result = $result")
    result.map(_.getOrElse(throw new IllegalStateException("Could not get DataFile ID from response")))
  }

  protected def getPathToFileInfo(deposit: Deposit): Try[Map[Path, FileInfo]] = {
    for {
      bagPathToFileInfo <- deposit.getPathToFileInfo
      pathToFileInfo = bagPathToFileInfo.map { case (bagPath, fileInfo) => Paths.get("data").relativize(bagPath) -> fileInfo }
      filteredPathToFileInfo = excludeFiles(pathToFileInfo)
    } yield filteredPathToFileInfo
  }

  private def excludeFiles(p2fi: Map[Path, FileInfo]): Map[Path, FileInfo] = {
    trace(p2fi)
    p2fi.toList.filter {
      case (p, _) =>
        val foundMatch = optFileExclusionPattern.forall(_.matcher(p.toString).matches())
        if (foundMatch) logger.info(s"Excluding file: ${ p.toString }")
        !foundMatch
    }.toMap
  }

  protected def updateFileMetadata(databaseIdToFileInfo: Map[Int, FileMeta]): Try[Unit] = {
    trace(databaseIdToFileInfo)
    databaseIdToFileInfo.map { case (id, fileMeta) =>
      val json = Serialization.write(fileMeta)
      debug(s"id = $id, json = $json")
      val r = Try(dataverseClient.file(id).updateMetadata(json))
      debug(s"id = $id, result = $r")
      r
    }.collectResults.map(_ => ())
  }

  protected def configureEnableAccessRequests(deposit: Deposit, persistendId: PersistentId, canEnable: Boolean): Try[Unit] = {
    for {
      ddm <- deposit.tryDdm
      files <- deposit.tryFilesXml
      enable = AccessRights.isEnableRequests((ddm \ "profile" \ "accessRights").head, files)
      _ = logger.trace("AccessRequests enable "+ enable + " can " +canEnable)
      _ <- if (!enable) Try(dataverseClient.accessRequests(persistendId).disable())
           else if (canEnable) Try(dataverseClient.accessRequests(persistendId).enable())
                else Success(())
    } yield ()
  }

  protected def licenseAsJson(supportedLicenses: List[URI])(variantToNormalized: Map[String, String])(deposit: Deposit): Try[String] = {
    trace(deposit)
    for {
      ddm <- deposit.tryDdm
      license <- (ddm \ "dcmiMetadata" \ "license").find(License.isLicenseUri).map(Success(_)).getOrElse(Failure(RejectedDepositException(deposit, "No license specified")))
      uri = License.getLicenseUri(supportedLicenses)(variantToNormalized)(license).toASCIIString
    } yield s"""
                |{ "http://schema.org/license": "$uri" }
                |""".stripMargin
  }

  protected def isEmbargo(date: Date): Boolean = {
    date.compareTo(new Date()) > 0
  }

  protected def embargoFiles(persistendId: PersistentId, dateAvailable: Date, fileIds: List[Int]): Try[Unit] = {
    trace(persistendId, fileIds)
    val embargo = new Embargo(dateAvailableFormat.format(dateAvailable), "", fileIds.toArray)
    Try(dataverseClient.dataset(persistendId).setEmbargo(embargo))
  }

  protected def deleteDraftIfExists(persistentId: String): Unit = {
    val result = for {
      v <- Try(dataverseClient.dataset(persistentId).getLatestVersion.getData)
      _ = logger.trace("deleting draft")
      _ <- if (v.getLatestVersion.getVersionState.contains("DRAFT"))
             deleteDraft(persistentId)
           else Success(())
    } yield ()
    result.doIfFailure {
      case e => logger.warn("Could not delete draft", e)
    }
  }

  private def deleteDraft(persistentId: PersistentId): Try[Unit] = {
    for {
      _ <- Try(dataverseClient.dataset(persistentId).deleteDraft())
      _ = logger.info(s"DRAFT deleted")
    } yield ()
  }
}
