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
import nl.knaw.dans.easy.dd2d.migrationinfo.BasicFileMeta
import nl.knaw.dans.lib.scaladv.model.dataset.Embargo
import nl.knaw.dans.lib.scaladv.model.file.FileMeta
import nl.knaw.dans.lib.scaladv.model.file.prestaged.PrestagedFile
import nl.knaw.dans.lib.scaladv.{ DatasetApi, DataverseInstance }
import nl.knaw.dans.lib.error.{ TraversableTryExtensions, TryExtensions }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import java.net.URI
import java.nio.file.{ Path, Paths }
import java.util.Date
import java.util.regex.Pattern
import scala.collection.mutable
import scala.util.{ Failure, Success, Try }

/**
 * Object that edits a dataset, a new draft.
 */
abstract class DatasetEditor(dataverseInstance: DataverseInstance, optFileExclusionPattern: Option[Pattern], zipFileHandler: ZipFileHandler) extends DebugEnhancedLogging {
  type PersistentId = String
  type DatasetId = Int

  /**
   * Performs the task.
   *
   * @return the persistentId of the dataset created or modified
   */
  def performEdit(): Try[PersistentId]

  protected def addFiles(persistentId: String, files: List[FileInfo], prestagedFiles: Set[BasicFileMeta] = Set.empty): Try[Map[Int, FileInfo]] = Try {
    trace(persistentId, files)
    val result = mutable.Map[Int, FileInfo]()
    for (f <- files) {
      debug(s"Adding file, directoryLabel = ${ f.metadata.directoryLabel }, label = ${ f.metadata.label }")
      val id = addFile(persistentId, f, prestagedFiles).get
      dataverseInstance.dataset(persistentId).awaitUnlock().unsafeGetOrThrow
      result(id) = f
    }
    result.toMap
  }

  private def addFile(doi: String, fileInfo: FileInfo, prestagedFiles: Set[BasicFileMeta]): Try[Int] = {
    val result = for {
      r <- getPrestagedFileFor(fileInfo, prestagedFiles).map { prestagedFile =>
        debug(s"Adding prestaged file: $fileInfo")
        dataverseInstance.dataset(doi).addPrestagedFile(prestagedFile)
      }.getOrElse {
        debug(s"Uploading file: $fileInfo")
        val optWrappedZip = zipFileHandler
          .wrapIfZipFile(fileInfo.file)
        val r = dataverseInstance.dataset(doi).addFile(Option(
          optWrappedZip
            .getOrElse(fileInfo.file)), Option(fileInfo.metadata))
        optWrappedZip.foreach(_.delete(swallowIOExceptions = true))
        r
      }
      files <- r.data
      id = files.files.headOption.flatMap(_.dataFile.map(_.id))
      _ <- dataverseInstance.dataset(doi).awaitUnlock()
    } yield id
    debug(s"Result = $result")
    result.map(_.getOrElse(throw new IllegalStateException("Could not get DataFile ID from response")))
  }

  protected def getPathToFileInfo(deposit: Deposit): Try[Map[Path, FileInfo]] = {
    for {
      bagPathToFileInfo <- deposit.getPathToFileInfo
      pathToFileInfo = bagPathToFileInfo.map { case (bagPath, fileInfo) => (Paths.get("data").relativize(bagPath) -> fileInfo) }
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

  protected def getPrestagedFileFor(fileInfo: FileInfo, basicFileMetas: Set[BasicFileMeta]): Option[PrestagedFile] = {
    val matchingChecksums = basicFileMetas.filter(_.prestagedFile.checksum.`@value` == fileInfo.checksum)
    if (matchingChecksums.size == 1) Option(matchingChecksums.head.prestagedFile)
    else if (matchingChecksums.isEmpty) Option.empty // no matches
         else { // multiple matches
           val matchingPaths = basicFileMetas.filter(bfm => bfm.label == fileInfo.metadata.label.get && bfm.directoryLabel == fileInfo.metadata.directoryLabel)
           if (matchingPaths.size == 1) Option(matchingPaths.head.prestagedFile)
           else if (matchingPaths.isEmpty) Option.empty
                else throw new IllegalArgumentException("Found multiple basic file metas with the same path in a single dataset version")
         }
  }

  protected def updateFileMetadata(databaseIdToFileInfo: Map[Int, FileMeta]): Try[Unit] = {
    trace(databaseIdToFileInfo)
    databaseIdToFileInfo.map { case (id, fileMeta) => {
      val r = dataverseInstance.file(id).updateMetadata(fileMeta)
      debug(s"id = $id, result = $r")
      r
    }
    }.collectResults.map(_ => ())
  }

  protected def configureEnableAccessRequests(deposit: Deposit, persistendId: PersistentId, canEnable: Boolean): Try[Unit] = {
    for {
      ddm <- deposit.tryDdm
      files <- deposit.tryFilesXml
      enable = AccessRights.isEnableRequests((ddm \ "profile" \ "accessRights").head, files)
      _ <- if (enable && canEnable) dataverseInstance.accessRequests(persistendId).enable()
           else Success(())
      _ <- if (!enable) dataverseInstance.accessRequests(persistendId).disable()
           else Success(())
    } yield ()
  }

  protected def setLicense(supportedLicenses: List[URI])(variantToNormalized: Map[String, String])(deposit: Deposit, dataset: DatasetApi): Try[Unit] = {
    trace(deposit)
    for {
      ddm <- deposit.tryDdm
      optLicense = (ddm \ "dcmiMetadata" \ "license").find(License.isLicenseUri)
      _ <- if (optLicense.isEmpty) Failure(RejectedDepositException(deposit, "No license specified"))
           else dataset.updateMetadataFromJsonLd(
             s"""
                |{ "http://schema.org/license": "${ License.getLicenseUri(supportedLicenses)(variantToNormalized)(optLicense.get).toASCIIString }" }
                |""".stripMargin, replace = true)
    } yield ()
  }

  protected def getFilesToEmbargo(persistendId: PersistentId): Try[List[FileMeta]] = {
    for {
      r <- dataverseInstance.dataset(persistendId).listFiles()
      files <- r.data
      filesToEmbargo = files.filter(f => f.directoryLabel.getOrElse("") != "easy-migration")
    } yield filesToEmbargo
  }

  protected def isEmbargo(date: Date): Boolean = {
    date.compareTo(new Date()) > 0
  }

  protected def embargoFiles(persistendId: PersistentId, dateAvailable: Date, fileIds: List[Int]): Try[Unit] = {
    trace(persistendId, fileIds)
    dataverseInstance.dataset(persistendId).setEmbargo(Embargo(dateAvailableFormat.format(dateAvailable), "", fileIds)).map(_ => ())
  }

  protected def deleteDraftIfExists(persistentId: String): Unit = {
    val result = for {
      r <- dataverseInstance.dataset(persistentId).viewLatestVersion()
      v <- r.data
      _ <- if (v.latestVersion.versionState.contains("DRAFT"))
             deleteDraft(persistentId)
           else Success(())
    } yield ()
    result.doIfFailure {
      case e => logger.warn("Could not delete draft", e)
    }
  }

  private def deleteDraft(persistentId: PersistentId): Try[Unit] = {
    for {
      r <- dataverseInstance.dataset(persistentId).deleteDraft()
      _ = logger.info(s"DRAFT deleted")
    } yield ()
  }
}
