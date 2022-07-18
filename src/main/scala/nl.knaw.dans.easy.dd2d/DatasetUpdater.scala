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

import nl.knaw.dans.lib.dataverse._
import nl.knaw.dans.lib.dataverse.model.dataset.FileList
import nl.knaw.dans.lib.dataverse.model.file.{ FileMeta => JavaFileMeta }
import nl.knaw.dans.lib.dataverse.model.search
import nl.knaw.dans.lib.error.{ TraversableTryExtensions, TryExtensions }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.lib.scaladv.model.dataset.MetadataBlocks
import org.json4s.native.Serialization

import java.net.URI
import java.nio.file.{ Path, Paths }
import java.util.Optional
import java.util.regex.Pattern
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

class DatasetUpdater(deposit: Deposit,
                     optFileExclusionPattern: Option[Pattern],
                     zipFileHandler: ZipFileHandler,
                     isMigration: Boolean = false,
                     metadataBlocks: MetadataBlocks,
                     variantToLicense: Map[String, String],
                     supportedLicenses: List[URI],
                     dataverseClient: DataverseClient) extends DatasetEditor(dataverseClient, optFileExclusionPattern, zipFileHandler) with DebugEnhancedLogging {
  trace(deposit)

  override def performEdit(): Try[PersistentId] = {
    {
      for {
        _ <- Try { Thread.sleep(4000) } // TODO wait for the doi to become available
        doi <- if (!isMigration) getDoi(s"""dansSwordToken:"${ deposit.vaultMetadata.dataverseSwordToken }"""")
               else {
                 // using deposit.dataversePid may lead to confusing situations when the DOI is present but erroneously so.
                 getDoiByIsVersionOf
               }
      } yield doi
    } match {
      case Failure(e) =>
        logger.error( s"Could not find persistentId of existing dataset ${deposit.bagDir} " + e)
        Failure(FailedDepositException(deposit, "Could not find persistentId of existing dataset", e))
      case Success(doi) => {
        for {
          javaDatasetApi <-  Try(dataverseClient.dataset(doi))
          _ <-  Try(javaDatasetApi.awaitUnlock())
          /*
           * Temporary fix. If we do not wait a couple of seconds here, the first version never gets properly published, and the second version
           * just overwrites it, becoming V1.
           */
          _ <- Try { Thread.sleep(8000) }
          // TODO: library should provide function waitForIndexing that uses the @Path("{identifier}/timestamps") endpoint on Datasets
          _ <-  Try(javaDatasetApi.awaitUnlock())
          state <- Try(javaDatasetApi.viewLatestVersion().getData.getLatestVersion.getVersionState)
          _ = if (state.contains("DRAFT")) throw CannotUpdateDraftDatasetException(deposit)

          jsonBlocks = Serialization.write(metadataBlocks)
          _ <- Try(javaDatasetApi.updateMetadataFromJsonLd(jsonBlocks, true))
          _ <- Try(javaDatasetApi.awaitUnlock())
          licenseAsJson <- licenseAsJson(supportedLicenses)(variantToLicense)(deposit)
          _ <- Try(javaDatasetApi.updateMetadataFromJsonLd(licenseAsJson, true))
          _ <- Try(javaDatasetApi.awaitUnlock())

          pathToFileInfo <- getPathToFileInfo(deposit)
          _ = debug(s"pathToFileInfo = $pathToFileInfo")
          pathToFileMetaInLatestVersion <- getFilesInLatestVersion(javaDatasetApi)
          _ = debug(s"pathToFileMetaInLatestVersion = $pathToFileMetaInLatestVersion")
          _ <- validateFileMetas(pathToFileMetaInLatestVersion.values.toList)

          versions <- Try(javaDatasetApi.getAllVersions.getData)
          numPub = versions.count(v => "RELEASED" == v.getVersionState)
          _ = debug(s"Number of published versions so far: $numPub")

          oldToNewPathMovedFiles <- getOldToNewPathOfFilesToMove(pathToFileMetaInLatestVersion, pathToFileInfo)
          fileMovements = oldToNewPathMovedFiles.map { case (old, newPath) => (pathToFileMetaInLatestVersion(old).getDataFile.getId, pathToFileInfo(newPath).javaFileMeta) }
          // Movement will be realized by updating label and directoryLabel attributes of the file; there is no separate "move-file" API endpoint.
          _ = debug(s"fileMovements = $fileMovements")

          /*
           * File replacement can only happen on files with paths that are not also involved in a rename/move action. Otherwise we end up with:
           *
           * - trying to update the file metadata by a database ID that is not the "HEAD" of a file version history (which Dataverse doesn't allow anyway, it
           * fails with "You cannot edit metadata on a dataFile that has been replaced"). This happens when a file A is renamed to B, but a different file A
           * is also added in the same update.
           *
           * - trying to add a file with a name that already exists. This happens when a file A is renamed to B, while B is also part of the latest version
           */
          fileReplacementCandidates = pathToFileMetaInLatestVersion
            .filterNot { case (path, _) => oldToNewPathMovedFiles.keySet.contains(path) } // remove old paths of moved files
            .filterNot { case (path, _) => oldToNewPathMovedFiles.values.toSet.contains(path) } // remove new paths of moved files
          filesToReplace <- getFilesToReplace(pathToFileInfo, fileReplacementCandidates)
          fileReplacements <- replaceFiles(javaDatasetApi, filesToReplace)
          _ = debug(s"fileReplacements = $fileReplacements")

          /*
           * To find the files to delete we start from the paths in the deposit payload. In principle, these paths are remaining, so should NOT be deleted.
           * However, if a file is moved/renamed to a path that was also present in the latest version, then the old file at that path must first be deleted
           * (and must therefore NOT included in candidateRemainingFiles). Otherwise we'll end up trying to use an existing (directoryLabel, label) pair.
           */
          candidateRemainingFiles = pathToFileInfo.keySet diff oldToNewPathMovedFiles.values.toSet

          /*
           * The paths to delete, now, are the paths in the latest version minus the remaining files. We further subtract the old paths of the moved files.
           * This may be a bit confusing, but the goals is to make sure that the underlying FILE remains present (after all, it is to be renamed/moved). The
           * path itself WILL be "removed" from the latest version by the move. (It MAY be filled again by a file addition in the same update, though.)
           */
          pathsToDelete = pathToFileMetaInLatestVersion.keySet diff candidateRemainingFiles diff oldToNewPathMovedFiles.keySet
          _ = debug(s"pathsToDelete = $pathsToDelete")
          fileDeletions <- getFileDeletions(pathsToDelete, pathToFileMetaInLatestVersion)
          _ = debug(s"fileDeletions = $fileDeletions")
          _ <- deleteFiles(javaDatasetApi, fileDeletions.toList)

          /*
           * After the movements have been performed, which paths are occupied? We start from the paths of the latest version (pathToFileMetaInLatestVersion.keySet)
           *
           * The old paths of the moved files (oldToNewPathMovedFiles.keySet) are no longer occupied, so they must be subtracted. This is important in the case where
           * a deposit renames/moves a file (leaving the checksum unchanges) but provides a new file for the vacated path.
           *
           * The paths of the deleted files (pathsToDelete) are no longer occupied, so must be subtracted. (It is not strictly necessary for the calculation
           * of pathsToAdd, but done anyway to keep the logic consistent.)
           *
           * The new paths of the moved files (oldToNewPathMovedFiles.values.toSet) *are* now occupied, so the must be added. This is important to
           * avoid those files from being marked as "new" files, i.e. files to be added.
           *
           * All paths in the deposit that are not occupied, are new files to be added.
           */
          occupiedPaths = (pathToFileMetaInLatestVersion.keySet diff oldToNewPathMovedFiles.keySet diff pathsToDelete) union oldToNewPathMovedFiles.values.toSet
          _ = debug(s"occupiedPaths = $occupiedPaths")
          pathsToAdd = pathToFileInfo.keySet diff occupiedPaths
          filesToAdd = pathsToAdd.map(pathToFileInfo).toList
          _ = debug(s"filesToAdd = $filesToAdd")
          fileAdditions <- addFiles(doi, filesToAdd).map(_.mapValues(_.javaFileMeta))

          // TODO: check that only updating the file metadata works
          _ <- updateFileMetadata(fileReplacements ++ fileMovements ++ fileAdditions)
          _ <- Try(javaDatasetApi.awaitUnlock())

          dateAvailable <- deposit.getDateAvailable
          _ <- if (isEmbargo(dateAvailable)) {
            val fileIdsToEmbargo = (fileReplacements ++ fileAdditions).filter(f => "easy-migration" != f._2.asInstanceOf[JavaFileMeta].getDirectoryLabel).keys
            logger.info(s"Embargoing new files until $dateAvailable")
            embargoFiles(doi, dateAvailable, fileIdsToEmbargo.toList)
          }
               else {
                 logger.debug(s"Date available in the past, no embargo: $dateAvailable }")
                 Success(())
               }
          /*
           * Cannot enable requests if they were disallowed because of closed files in a previous version. However disabling is possible because a the update may add a closed file.
           */
          _ <- configureEnableAccessRequests(deposit, doi, canEnable = false)
        } yield doi
      }.doIfFailure {
        case _: CannotUpdateDraftDatasetException => // Don't delete the draft that caused the failure
        case NonFatal(e) =>
          logger.error(s"Dataset update failed, deleting draft", e)
          deleteDraftIfExists(doi)
      }
    }
  }

  private def getDoi(q: String) = {
    logger.debug(q)
    for {
      searchResult <- Try(dataverseClient.search().find(q).getData)
      items = searchResult.getItems
      _ = if (items.size != 1) throw FailedDepositException(deposit, s"Deposit is update of ${ items.size } datasets; should always be 1!")
      doi = items.head.asInstanceOf[search.DatasetResultItem].getGlobalId
      _ = debug(s"Deposit is update of dataset $doi")
    } yield doi
  }

  private def getDoiByIsVersionOf: Try[String] = {
    for {
      isVersionOf <- deposit.getIsVersionOf
      doi <- getDoi(s"""dansBagId:"$isVersionOf"""")
    } yield doi
  }

  private def getFilesInLatestVersion(dataset: DatasetApi): Try[Map[Path, JavaFileMeta]] = {
    for {
      response <- Try(dataset.getFiles(Version.LATEST_PUBLISHED.toString)) // N.B. If LATEST_PUBLISHED is not specified, it almost works, but the directoryLabel is not picked up somehow.
      files <- Try(response.getData)
      pathToFileMeta = files.map { javaFileMeta =>
        (getPathFromFileMeta(javaFileMeta), javaFileMeta)
      }.toMap
    } yield pathToFileMeta
  }

  private def getPathFromFileMeta(fileMeta: JavaFileMeta): Path = {
    Paths.get(fileMeta.getDirectoryLabel, fileMeta.getLabel)
  }

  private def validateFileMetas(files: List[JavaFileMeta]): Try[Unit] = {
    if (files.map(_.getDataFile).contains(null))
      Failure(new IllegalArgumentException("Found file metadata without dataFile element"))
    else if (files.map(_.getDataFile).exists("SHA-1" != _.getChecksum.getType))
               Failure(new IllegalArgumentException("Not all file checksums are of type SHA-1"))
         else Success(())
  }

  private def getFilesToReplace(pathToFileInfo: Map[Path, FileInfo], pathToFileMetaInLatestVersion: Map[Path, JavaFileMeta]): Try[Map[Int, FileInfo]] = Try {
    trace(())
    val intersection = pathToFileInfo.keySet intersect pathToFileMetaInLatestVersion.keySet
    debug(s"The following files are in both deposit and latest published version: ${ intersection.mkString(", ") }")
    val checksumsDiffer = intersection.filter(p => pathToFileInfo(p).checksum != pathToFileMetaInLatestVersion(p).getDataFile.getChecksum.getValue)
    debug(s"The following files are in both deposit and latest published version AND have a different checksum: ${ checksumsDiffer.mkString(", ") }")
    checksumsDiffer.map(p => (pathToFileMetaInLatestVersion(p).getDataFile.getId, pathToFileInfo(p))).toMap
  }

  /**
   * Creatings a mapping for moving files to a new location. To determine this, the file needs to be unique in the old and the new version, because
   * its checksum is used to locate it. Files that occur multiple times in either the old or the new version cannot be moved in this way. They
   * will appear to have been deleted in the old version and added in the new. This has the same net result, except that the "Changes" overview in
   * Dataverse does not record that the file was effectively moved.
   *
   * @param pathToFileMetaInLatestVersion map from path to file metadata in the old version
   * @param pathToFileInfo                map from path to file info in the new version (i.e. the deposit).
   * @return
   */
  private def getOldToNewPathOfFilesToMove(pathToFileMetaInLatestVersion: Map[Path, JavaFileMeta], pathToFileInfo: Map[Path, FileInfo]): Try[Map[Path, Path]] = {
    for {
      checksumsToPathNonDuplicatedFilesInDeposit <- getChecksumsToPathOfNonDuplicateFiles(pathToFileInfo.mapValues(_.checksum))
      checksumsToPathNonDuplicatedFilesInLatestVersion <- getChecksumsToPathOfNonDuplicateFiles(pathToFileMetaInLatestVersion.mapValues(_.getDataFile.getChecksum.getValue))
      checksumsOfPotentiallyMovedFiles = checksumsToPathNonDuplicatedFilesInDeposit.keySet intersect checksumsToPathNonDuplicatedFilesInLatestVersion.keySet
      oldToNewPathMovedFiles = checksumsOfPotentiallyMovedFiles
        .map(c => (checksumsToPathNonDuplicatedFilesInLatestVersion(c), checksumsToPathNonDuplicatedFilesInDeposit(c)))
      /*
       * Work-around for a bug in Dataverse. The API seems to lose the directoryLabel when the draft of a second version is started. For now, we therefore don't filter
       * away files that have kept the same path. They will be "moved" in place, making sure the directoryLabel is reconfirmed.
       *
       * For files with duplicates in the same dataset this will not work, because those are not collected above.
       */
      //        .filter { case (pathInLatestVersion, pathInDeposit) => pathInLatestVersion != pathInDeposit }
    } yield oldToNewPathMovedFiles.toMap
  }

  private def getChecksumsToPathOfNonDuplicateFiles(pathToChecksum: Map[Path, String]): Try[Map[String, Path]] = Try {
    pathToChecksum
      .groupBy { case (_, c) => c }
      .filter { case (_, pathToFileInfoMappings) => pathToFileInfoMappings.size == 1 }
      .map { case (c, m) => (c, m.head._1) }
  }

  private def getFileDeletions(paths: Set[Path], pathToFileMeta: Map[Path, JavaFileMeta]): Try[Set[Int]] = Try {
    paths.map(path => pathToFileMeta(path).getDataFile.getId)
  }

  private def deleteFiles(dataset: DatasetApi, databaseIds: List[DatabaseId]): Try[Unit] = {
    databaseIds.map { id =>
      debug(s"Deleting file, databaseId = $id")
      dataverseClient.sword().deleteFile(id)
      Try(dataset.awaitUnlock())
    }.collectResults.map(_ => ())
  }

  private def replaceFiles(dataset: DatasetApi, databaseIdToNewFile: Map[Int, FileInfo]): Try[Map[Int, JavaFileMeta]] = {
    trace(databaseIdToNewFile)
    databaseIdToNewFile.map {
      case (id, fileInfo) =>
        val fileApi = dataverseClient.file(id)

        def replaceFile(): Try[(Int, JavaFileMeta)] = {
          /*
           * Note, forceReplace = true is used, so that the action does not fail if the replacement has a different MIME-type than
           * the replaced file. The only way to pass forceReplace is through the FileMeta. This means we are deleting any existing
           * metadata with the below call. This is not a problem, because the metadata will be made up-to-date at the end of the
           * update process.
           */
          for {
            r: DataverseHttpResponse[FileList] <- {
              val meta = new JavaFileMeta()
              meta.setForceReplace(true)
              val json = Serialization.writePretty(meta)
              debug(s"Uploading replacement file: $fileInfo $json")
              Try(fileApi.replaceFileItem(Optional.of(fileInfo.file.toJava), Optional.of(json)))
            }
            fileList <- Try(r.getData)
            id = Try(fileList.getFiles.get(0).getDataFile.getId)
              .getOrElse(throw new IllegalStateException("Could not get ID of replacement file after replace action"))
          } yield (id, fileInfo.javaFileMeta)
        }
        for {
          (replacementId, replacementMeta) <- replaceFile()
          _ <- Try(dataset.awaitUnlock())
        } yield (replacementId, replacementMeta)
    }.toList.collectResults.map(_.toMap)
  }
}
