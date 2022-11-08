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
import nl.knaw.dans.easy.dd2d.Deposit;
import nl.knaw.dans.easy.dd2d.FailedDepositException;
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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    public String performEdit() {
        // TODO wait for the doi to become available (uses sleep in scala, but this was frowned upon!)
        try {
            var doi = isMigration
                ? getDoiByIsVersionOf()
                : getDoi(String.format("dansSwordToken:%s", deposit.vaultMetadata().dataverseSwordToken()));

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

            var license = toJson(Map.of("http://schema.org/license", getLicense(deposit.tryDdm().get())));
            api.updateMetadataFromJsonLd(license, true);
            api.awaitUnlock();

            var pathToFileInfo = getFileInfo();
            log.debug("pathToFileInfo = {}", pathToFileInfo);
            var pathToFileInfoInLatestVersion = getFilesInfoInLatestVersion(api);

            validateFileMetas(pathToFileInfoInLatestVersion);


            // version
            var versions = api.getAllVersions().getData();
            var publishedVersions = versions.stream().filter(v -> v.getVersionState().equals("RELEASED")).count();
            log.debug("Number of published versions so far: {}", publishedVersions);

            // move old paths to new paths

            /*
            oldToNewPathMovedFiles <- getOldToNewPathOfFilesToMove(pathToFileMetaInLatestVersion, pathToFileInfo)
            fileMovements = oldToNewPathMovedFiles.map { case (old, newPath) => (pathToFileMetaInLatestVersion(old).getDataFile.getId, pathToFileInfo(newPath).metadata) }
            // Movement will be realized by updating label and directoryLabel attributes of the file; there is no separate "move-file" API endpoint.
            _ = debug(s"fileMovements = $fileMovements")
             */

            // embargo
            // TODO only embargo a subset of files based on previous actions
            var dateAvailable = deposit.getDateAvailable().get();
            embargoFiles(doi, dateAvailable);

        }
        catch (Exception e) {
            log.error("Error updating dataset", e);
        }

        return null;
    }

    void getOldToNewPathOfFilesToMove(Map<Path, FileMeta> pathToFileMetaInLatestVersion, Map<Path, FileInfo> pathToFileInfo) {
        
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
            if (fileMeta.getDataFile().getChecksum().getType().equals("SHA-1")) {
                throw new IllegalArgumentException("Not all file checksums are of type SHA-1");
            }
        }
    }

    private Map<Path, FileMeta> getFilesInfoInLatestVersion(DatasetApi datasetApi) throws IOException, DataverseException {
        var response = datasetApi.getFiles(Version.LATEST_PUBLISHED.toString());
        var entries = response.getData().stream()
            .map(item -> {
                var path = Path.of(item.getDirectoryLabel(), item.getLabel());
                return Map.entry(path, item);
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return entries;
    }

    private String getDoiByIsVersionOf() throws IOException, DataverseException {
        var isVersionOf = deposit.getIsVersionOf();
        return getDoi(String.format("dansBagId:%s", isVersionOf.get()));
    }

    String getDoi(String query) throws IOException, DataverseException {
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
