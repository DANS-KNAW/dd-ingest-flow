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
import nl.knaw.dans.ingest.core.dataverse.DatasetService;
import nl.knaw.dans.ingest.core.domain.Deposit;
import nl.knaw.dans.ingest.core.domain.FileInfo;
import nl.knaw.dans.ingest.core.exception.FailedDepositException;
import nl.knaw.dans.lib.dataverse.DataverseApi;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.RoleAssignment;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@Slf4j
public class DatasetCreator extends DatasetEditor {

    private final String depositorRole;

    public DatasetCreator(
        boolean isMigration,
        Dataset dataset,
        Deposit deposit,
        ObjectMapper objectMapper,
        List<URI> supportedLicenses,
        Pattern fileExclusionPattern,
        ZipFileHandler zipFileHandler,
        String depositorRole,
        DatasetService datasetService
    ) {
        super(
            isMigration,
            dataset,
            deposit,
            supportedLicenses,
            fileExclusionPattern,
            zipFileHandler, objectMapper, datasetService);

        this.depositorRole = depositorRole;
    }

    @Override
    public String performEdit() {
        var api = dataverseClient.dataverse("root");

        log.info("Creating new dataset");

        try {
            var persistentId = importDataset(api);
            log.debug("New persistent ID: {}", persistentId);
            modifyDataset(persistentId);
            return persistentId;
        }
        catch (Exception e) {
            throw new FailedDepositException(deposit, "Error creating dataset, deleting draft", e);
        }
    }

    private void modifyDataset(String persistentId) throws IOException, DataverseException {
        var api = dataverseClient.dataset(persistentId);

        // This will set fileAccessRequest and termsOfAccess
        var version = dataset.getDatasetVersion();
        version.setFileAccessRequest(deposit.allowAccessRequests());
        if (!deposit.allowAccessRequests() && StringUtils.isBlank(version.getTermsOfAccess())) {
            version.setTermsOfAccess("N/a");
        }
        api.updateMetadata(version);
        api.awaitUnlock();

        // license stuff
        var license = toJson(Map.of("http://schema.org/license", getLicense(deposit.getDdm())));
        log.info("Setting license to {}", license);
        api.updateMetadataFromJsonLd(license, true);
        api.awaitUnlock();

        // add files to dataset
        var pathToFileInfo = getFileInfo();
        log.debug("File info: {}", pathToFileInfo);
        var databaseIds = addFiles(persistentId, pathToFileInfo.values());

        log.debug("Database ID's: {}", databaseIds);
        // update individual files metadata
        updateFileMetadata(databaseIds);
        api.awaitUnlock();

        api.assignRole(getRoleAssignment());
        api.awaitUnlock();

        var dateAvailable = getDateAvailable(deposit); //deposit.getDateAvailable().get();
        embargoFiles(persistentId, dateAvailable);
    }

    private RoleAssignment getRoleAssignment() {
        var result = new RoleAssignment();
        result.setRole(depositorRole);
        result.setAssignee(String.format("@%s", deposit.getDepositorUserId()));

        return result;
    }

    private void updateFileMetadata(Map<Integer, FileInfo> databaseIds) throws IOException, DataverseException {
        for (var entry : databaseIds.entrySet()) {
            var id = entry.getKey();
            var fileMeta = objectMapper.writeValueAsString(entry.getValue().getMetadata());

            log.debug("id = {}, json = {}", id, fileMeta);
            var result = dataverseClient.file(id).updateMetadata(fileMeta);
            log.debug("id = {}, result = {}", id, result);
        }
    }

    String importDataset(DataverseApi api) throws IOException, DataverseException {
        var response = isMigration
            ? api.importDataset(dataset, Optional.of(String.format("doi:%s", deposit.getDoi())), false)
            : api.createDataset(dataset);

        return response.getData().getPersistentId();
    }
}
