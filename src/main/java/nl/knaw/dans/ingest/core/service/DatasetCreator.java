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
import nl.knaw.dans.ingest.core.service.exception.FailedDepositException;
import nl.knaw.dans.lib.dataverse.DataverseApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.RoleAssignment;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;

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
        DataverseClient dataverseClient,
        boolean isMigration,
        Dataset dataset,
        Deposit deposit,
        ObjectMapper objectMapper,
        Map<String, String> variantToLicense,
        List<URI> supportedLicenses,
        int publishAwaitUnlockMillisecondsBetweenRetries,
        int publishAwaitUnlockMaxNumberOfRetries,
        Pattern fileExclusionPattern,
        ZipFileHandler zipFileHandler,
        String depositorRole) {
        super(dataverseClient,
            isMigration,
            dataset,
            deposit,
            variantToLicense,
            supportedLicenses,
            publishAwaitUnlockMillisecondsBetweenRetries,
            publishAwaitUnlockMaxNumberOfRetries,
            fileExclusionPattern,
            zipFileHandler, objectMapper);

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
            log.error("Error creating dataset", e);
            throw new FailedDepositException(deposit, "could not import/create dataset", e);
        }
    }

    private void modifyDataset(String persistentId) throws IOException, DataverseException {
        var api = dataverseClient.dataset(persistentId);

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

        configureEnableAccessRequests(persistentId, true);
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
        // TODO check if we need to return the results; in the scala version it does return
        // but the results are never used
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
