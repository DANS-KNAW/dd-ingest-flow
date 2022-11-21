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
import nl.knaw.dans.easy.dd2d.mapping.AccessRights;
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

        try {
            var persistentId = importDataset(api);
            modifyDataset(persistentId);
            return persistentId;
        }
        catch (Exception e) {
            throw new FailedDepositException(deposit, "could not import/create dataset", e);
        }
    }

    private void modifyDataset(String persistentId) throws IOException, DataverseException {
        var api = dataverseClient.dataset(persistentId);

        // license stuff
        var license = toJson(Map.of("http://schema.org/license", getLicense(deposit.tryDdm().get())));
        api.updateMetadataFromJsonLd(license, true);
        api.awaitUnlock(publishAwaitUnlockMaxNumberOfRetries, publishAwaitUnlockMillisecondsBetweenRetries);

        // add files to dataset
        var pathToFileInfo = getFileInfo();
        var databaseIds = addFiles(persistentId, pathToFileInfo.values());

        // update individual files metadata
        updateFileMetadata(databaseIds);
        api.awaitUnlock(publishAwaitUnlockMaxNumberOfRetries, publishAwaitUnlockMillisecondsBetweenRetries);

        configureEnableAccessRequests(persistentId, true);
        api.awaitUnlock(publishAwaitUnlockMaxNumberOfRetries, publishAwaitUnlockMillisecondsBetweenRetries);

        api.assignRole(getRoleAssignment());
        api.awaitUnlock(publishAwaitUnlockMaxNumberOfRetries, publishAwaitUnlockMillisecondsBetweenRetries);

        var dateAvailable = deposit.getDateAvailable().get();
        embargoFiles(persistentId, dateAvailable);
    }

    private RoleAssignment getRoleAssignment() {
        var result = new RoleAssignment();
        result.setRole(depositorRole);
        result.setAssignee(String.format("@%s", deposit.getDepositorUserId()));

        return result;
    }

    private void configureEnableAccessRequests(String persistentId, boolean canEnable) throws IOException, DataverseException {
        var api = dataverseClient.accessRequests(persistentId);

        var ddm = deposit.tryDdm().get();
        var files = deposit.tryFilesXml().get();
        var enable = AccessRights.isEnableRequests(ddm.$bslash("profile").$bslash("accessRights").head(), files);

        if (!enable) {
            api.disable();
        }
        else if (canEnable) {
            api.enable();
        }
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
