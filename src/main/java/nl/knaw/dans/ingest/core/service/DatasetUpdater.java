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
package nl.knaw.dans.ingest.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.core.domain.Deposit;
import nl.knaw.dans.ingest.core.domain.FileInfo;
import nl.knaw.dans.ingest.core.exception.CannotUpdateDraftDatasetException;
import nl.knaw.dans.ingest.core.exception.FailedDepositException;
import nl.knaw.dans.lib.dataverse.DatasetApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.Version;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlock;
import nl.knaw.dans.lib.dataverse.model.file.FileMeta;
import nl.knaw.dans.lib.dataverse.model.search.DatasetResultItem;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class DatasetUpdater extends DatasetEditor {
    private final Map<String, MetadataBlock> metadataBlocks;

    protected DatasetUpdater(DataverseClient dataverseClient, boolean isMigration, Dataset dataset,
        Deposit deposit, Map<String, String> variantToLicense, List<URI> supportedLicenses, int publishAwaitUnlockMillisecondsBetweenRetries,
        int publishAwaitUnlockMaxNumberOfRetries, Pattern fileExclusionPattern, ZipFileHandler zipFileHandler,
        ObjectMapper objectMapper, Map<String, MetadataBlock> metadataBlocks) {
        super(dataverseClient, isMigration, dataset, deposit, variantToLicense, supportedLicenses, publishAwaitUnlockMillisecondsBetweenRetries, publishAwaitUnlockMaxNumberOfRetries,
            fileExclusionPattern,
            zipFileHandler, objectMapper);
        this.metadataBlocks = metadataBlocks;
    }

    @Override
    public String performEdit() throws IOException, DataverseException, InterruptedException {
        // TODO wait for the doi to become available (uses sleep in scala, but this was frowned upon!)
        try {
            Thread.sleep(4000);

            var doi = isMigration
                ? getDoiByIsVersionOf()
                : getDoi(String.format("dansSwordToken:%s", deposit.getVaultMetadata().getSwordToken()));

            try {
                var api = dataverseClient.dataset(doi);
                api.awaitUnlock();

                // wait for dataverse to do stuff
                Thread.sleep(8000);
                api.awaitUnlock();

                var state = api.getLatestVersion().getData().getLatestVersion().getVersionState();

                if (state.contains("DRAFT")) {
                    throw new CannotUpdateDraftDatasetException(deposit);
                }

                var blocks = objectMapper.writeValueAsString(metadataBlocks);
                api.updateMetadataFromJsonLd(blocks, true);
                api.awaitUnlock();

                var license = toJson(Map.of("http://schema.org/license", getLicense(deposit.getDdm())));
                api.updateMetadataFromJsonLd(license, true);
                api.awaitUnlock();

                var pathToFileInfo = getFileInfo();
                log.debug("pathToFileInfo = {}", pathToFileInfo);
                var pathToFileMetaInLatestVersion = getFilesInfoInLatestVersion(api);

                validateFileMetas(pathToFileMetaInLatestVersion);

                var versions = api.getAllVersions().getData();
                var publishedVersions = versions.stream().filter(v -> v.getVersionState().equals("RELEASED")).count();
                log.debug("Number of published versions so far: {}", publishedVersions);

                // move old paths to new paths
                var oldToNewPathMovedFiles = getOldToNewPathOfFilesToMove(pathToFileMetaInLatestVersion, pathToFileInfo);
                var fileMovements = oldToNewPathMovedFiles.keySet().stream()
                    .map(path -> Map.entry(pathToFileMetaInLatestVersion.get(path).getDataFile().getId(), pathToFileInfo.get(oldToNewPathMovedFiles.get(path)).getMetadata()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                log.debug("fileMovements = {}", fileMovements);

                /*
                 * File replacement can only happen on files with paths that are not also involved in a rename/move action. Otherwise, we end up with:
                 *
                 * - trying to update the file metadata by a database ID that is not the "HEAD" of a file version history (which Dataverse doesn't allow anyway, it
                 * fails with "You cannot edit metadata on a dataFile that has been replaced"). This happens when a file A is renamed to B, but a different file A
                 * is also added in the same update.
                 *
                 * - trying to add a file with a name that already exists. This happens when a file A is renamed to B, while B is also part of the latest version
                 */
                var fileReplacementCandidates = pathToFileMetaInLatestVersion.entrySet().stream()
                    .filter(pathToFileInfoEntry -> !oldToNewPathMovedFiles.containsKey(pathToFileInfoEntry.getKey()))
                    .filter(pathToFileInfoEntry -> !oldToNewPathMovedFiles.containsValue(pathToFileInfoEntry.getKey())) // TODO in the OG version, this was a Set; check what performance is like
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                var filesToReplace = getFilesToReplace(pathToFileInfo, fileReplacementCandidates);
                log.debug("filesToReplace = {}", filesToReplace);
                var fileReplacements = replaceFiles(api, filesToReplace);
                log.debug("fileReplacements = {}", fileReplacements);

                /*
                 * To find the files to delete we start from the paths in the deposit payload. In principle, these paths are remaining, so should NOT be deleted.
                 * However, if a file is moved/renamed to a path that was also present in the latest version, then the old file at that path must first be deleted
                 * (and must therefore NOT be included in candidateRemainingFiles). Otherwise, we'll end up trying to use an existing (directoryLabel, label) pair.
                 */
                var oldPathsOfMovedFiles = new HashSet<>(oldToNewPathMovedFiles.keySet());
                var candidateRemainingFiles = diff(pathToFileInfo.keySet(), oldPathsOfMovedFiles);


                /*
                 * The paths to delete, now, are the paths in the latest version minus the remaining files. We further subtract the old paths of the moved files.
                 * This may be a bit confusing, but the goals is to make sure that the underlying FILE remains present (after all, it is to be renamed/moved). The
                 * path itself WILL be "removed" from the latest version by the move. (It MAY be filled again by a file addition in the same update, though.)
                 */
                var pathsToDelete = diff(diff(pathToFileMetaInLatestVersion.keySet(), candidateRemainingFiles), oldPathsOfMovedFiles);
                log.debug("pathsToDelete = {}", pathsToDelete);

                var fileDeletions = getFileDeletions(pathsToDelete, pathToFileMetaInLatestVersion);
                log.debug("fileDeletions = {}", fileDeletions);

                deleteFiles(api, fileDeletions);

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

                var diffed = diff(diff(pathToFileMetaInLatestVersion.keySet(), oldPathsOfMovedFiles), pathsToDelete);
                var occupiedPaths = union(diffed, oldToNewPathMovedFiles.values());

                log.debug("occupiedPaths = {}", occupiedPaths);
                var pathsToAdd = diff(pathToFileInfo.keySet(), occupiedPaths);
                var filesToAdd = pathsToAdd.stream().map(pathToFileInfo::get).collect(Collectors.toList());
                var fileAdditions = addFiles(doi, filesToAdd).entrySet().stream()
                    .map(e -> Map.entry(e.getKey(), e.getValue().getMetadata()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                // TODO: check that only updating the file metadata works (from scala code)
                updateFileMetadata(fileReplacements, fileMovements, fileAdditions);
                api.awaitUnlock();

                // embargo
                var dateAvailable = getDateAvailable(deposit);
                var fileIdsToEmbargo = union(fileReplacements.keySet(), fileAdditions.keySet())
                    .stream()
                    .map(key -> Map.entry(key, fileReplacements.getOrDefault(key, fileAdditions.get(key))))
                    .filter(entry -> !"easy-migration.zip".equals(entry.getValue().getLabel()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

                embargoFiles(doi, dateAvailable, fileIdsToEmbargo);

                /*
                 * Cannot enable requests if they were disallowed because of closed files in a previous version. However disabling is possible because a the update may add a closed file.
                 */
                configureEnableAccessRequests(doi, false);

                return doi;
            }
            catch (Exception e) {
                log.error("Error updating dataset, deleting draft", e);
                deleteDraftIfExists(doi);
                throw e;
            }
        }
        catch (Exception e) {
            log.error("Error updating dataset", e);
            throw e;
        }
    }

    void deleteDraftIfExists(String persistentId) throws IOException, DataverseException {
        var data = dataverseClient.dataset(persistentId).getLatestVersion().getData();

        if (data.getLatestVersion().getVersionState().contains("DRAFT")) {
            deleteDraft(persistentId);
        }
    }

    private void deleteDraft(String persistentId) throws IOException, DataverseException {
        dataverseClient.dataset(persistentId).deleteDraft();
    }

    @SafeVarargs
    private void updateFileMetadata(Map<Integer, FileMeta>... fileMaps) throws IOException, DataverseException {
        var seen = new HashSet<Integer>();

        for (var fileMap : fileMaps) {

            for (var file : fileMap.entrySet()) {
                var id = file.getKey();
                var fileMeta = file.getValue();

                // don't do duplicates
                if (seen.contains(id)) {
                    continue;
                }

                seen.add(id);

                var json = objectMapper.writeValueAsString(fileMeta);
                log.debug("id = {}, json = {}", id, json);

                var result = dataverseClient.file(id).updateMetadata(json);
                log.debug("id = {}, result = {}", id, result);
            }
        }
    }

    private void deleteFiles(DatasetApi api, Set<Integer> fileDeletions) throws IOException, DataverseException {
        for (var id : fileDeletions) {
            log.debug("Deleting file, databaseId = {}", id);
            dataverseClient.sword().deleteFile(id);
            api.awaitUnlock();
        }
    }

    private Set<Integer> getFileDeletions(Set<Path> pathsToDelete, Map<Path, FileMeta> pathToFileInfoInLatestVersion) {
        return pathsToDelete.stream()
            .map(p -> pathToFileInfoInLatestVersion.get(p).getDataFile().getId())
            .collect(Collectors.toSet());
    }

    <T> Set<T> diff(Collection<T> a, Collection<T> b) {
        return a.stream().filter(k -> !b.contains(k)).collect(Collectors.toSet());
    }

    <T> Set<T> intersection(Collection<T> a, Collection<T> b) {
        return a.stream().filter(b::contains).collect(Collectors.toSet());
    }

    <T> Set<T> union(Collection<T> a, Collection<T> b) {
        return Stream.of(a.stream(), b.stream()).flatMap(i -> i).collect(Collectors.toSet());
        // a.stream().filter(b::contains).collect(Collectors.toSet());
    }

    private Map<Integer, FileMeta> replaceFiles(DatasetApi api, Map<Integer, FileInfo> filesToReplace) throws IOException, DataverseException {
        var results = new HashMap<Integer, FileMeta>();

        for (var entry : filesToReplace.entrySet()) {
            log.debug("Replacing file with ID = {}", entry.getKey());
            var fileApi = dataverseClient.file(entry.getKey());

            var meta = new FileMeta();
            meta.setForceReplace(true);
            var json = objectMapper.writeValueAsString(meta);
            var wrappedZip = zipFileHandler.wrapIfZipFile(entry.getValue().getPath());
            var file = wrappedZip.orElse(entry.getValue().getPath());

            log.debug("Calling replaceFileItem({}, {})", entry.getValue().getPath(), json);
            var result = fileApi.replaceFileItem(
                Optional.of(file.toFile()),
                Optional.of(json)
            );

            if (wrappedZip.isPresent()) {
                try {
                    Files.deleteIfExists(wrappedZip.get());
                }
                catch (IOException e) {
                    log.error("Unable to delete zipfile {}", wrappedZip.get(), e);
                }
            }

            var id = -1;

            try {
                id = result.getData().getFiles().get(0).getDataFile().getId();
            }
            // TODO figure out what kind of exceptions can be thrown here, besides DataverseException and NPE
            catch (Throwable e) {
                log.error("Unable to get ID from result", e);
                throw new IllegalStateException("Could not get ID of replacement file after replace action", e);
            }

            results.put(id, entry.getValue().getMetadata());
            api.awaitUnlock();
        }

        return results;
    }

    private Map<Integer, FileInfo> getFilesToReplace(Map<Path, FileInfo> pathToFileInfo, Map<Path, FileMeta> fileReplacementCandidates) {

        var intersection = intersection(pathToFileInfo.keySet(), fileReplacementCandidates.keySet());

        log.debug("Intersection paths for replacing = {}", intersection);

        return intersection.stream()
            .filter(p -> !pathToFileInfo.get(p).getChecksum().equals(fileReplacementCandidates.get(p).getDataFile().getChecksum().getValue()))
            .map(p -> Map.entry(fileReplacementCandidates.get(p).getDataFile().getId(), pathToFileInfo.get(p)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Creatings a mapping for moving files to a new location. To determine this, the file needs to be unique in the old and the new version, because its checksum is used to locate it. Files that
     * occur multiple times in either the old or the new version cannot be moved in this way. They will appear to have been deleted in the old version and added in the new. This has the same net
     * result, except that the "Changes" overview in Dataverse does not record that the file was effectively moved.
     *
     * @param pathToFileMetaInLatestVersion map from path to file metadata in the old version
     * @param pathToFileInfo                map from path to file info in the new version (i.e. the deposit).
     * @return
     */
    Map<Path, Path> getOldToNewPathOfFilesToMove(Map<Path, FileMeta> pathToFileMetaInLatestVersion, Map<Path, FileInfo> pathToFileInfo) {

        var depositChecksums = pathToFileInfo.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().getChecksum()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        var latestFileChecksums = pathToFileMetaInLatestVersion.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().getDataFile().getChecksum().getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        var checksumsToPathNonDuplicatedFilesInDeposit = getChecksumsToPathOfNonDuplicateFiles(depositChecksums);
        var checksumsToPathNonDuplicatedFilesInLatestVersion = getChecksumsToPathOfNonDuplicateFiles(latestFileChecksums);

        var intersects = checksumsToPathNonDuplicatedFilesInDeposit.keySet().stream()
            .filter(checksumsToPathNonDuplicatedFilesInLatestVersion::containsKey)
            .collect(Collectors.toSet());

        return intersects.stream()
            .map(c -> Map.entry(checksumsToPathNonDuplicatedFilesInLatestVersion.get(c), checksumsToPathNonDuplicatedFilesInDeposit.get(c)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    }

    private Map<String, Path> getChecksumsToPathOfNonDuplicateFiles(Map<Path, String> pathToChecksum) {
        // inverse map first
        var inversed = pathToChecksum.entrySet().stream()
            .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

        // filter out items with 0 or more than 1 items
        return inversed.entrySet().stream()
            .filter(item -> item.getValue().size() == 1)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
    }

    private void validateFileMetas(Map<Path, FileMeta> pathToFileInfoInLatestVersion) {
        // check for nulls
        for (var fileMeta : pathToFileInfoInLatestVersion.values()) {
            if (fileMeta.getDataFile() == null) {
                throw new IllegalArgumentException("Found file metadata without dataFile element");
            }
        }

        // check if any of them have a checksum that is not SHA-1
        for (var fileMeta : pathToFileInfoInLatestVersion.values()) {
            var checksumType = fileMeta.getDataFile().getChecksum().getType();
            log.trace("Filemeta checksum type for file {}: {}", fileMeta.getLabel(), checksumType);
            if (!checksumType.equals("SHA-1")) {
                throw new IllegalArgumentException("Not all file checksums are of type SHA-1");
            }
        }
    }

    private Map<Path, FileMeta> getFilesInfoInLatestVersion(DatasetApi datasetApi) throws IOException, DataverseException {
        // N.B. If LATEST_PUBLISHED is not specified, it almost works, but the directoryLabel is not picked up somehow.
        // N.B.2 it still returns an empty (null) directoryLabel if there is no directoryLabel (file is in root of file structure)
        var response = datasetApi.getFiles(Version.LATEST_PUBLISHED.toString());

        return response.getData().stream()
            .map(item -> {
                try {
                    log.trace("File item = {}", objectMapper.writeValueAsString(item));
                    var path = Path.of(
                        Optional.ofNullable(item.getDirectoryLabel()).orElse(""),
                        item.getLabel()
                    );
                    return Map.entry(path, item);
                }
                catch (Exception e) {
                    log.error("Error converting json", e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private String getDoiByIsVersionOf() throws IOException, DataverseException {
        var isVersionOf = deposit.getIsVersionOf();
        if (isVersionOf == null)
            throw new IllegalArgumentException("Update-deposit without Is-Version-Of");
        return getDoi(String.format("dansBagId:\"%s\"", isVersionOf));
    }

    String getDoi(String query) throws IOException, DataverseException {
        log.trace("searching dataset with query '{}'", query);
        var results = dataverseClient.search().find(query);
        var items = results.getData().getItems();
        var count = items.size();

        if (count != 1) {
            throw new FailedDepositException(deposit, String.format(
                "Deposit is update of %s datasets; should always be 1!", count
            ), null);
        }

        var doi = ((DatasetResultItem) items.get(0)).getGlobalId();
        log.debug("Deposit is update of dataset {}", doi);

        return doi;
    }
}
