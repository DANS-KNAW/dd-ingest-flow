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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ingest.api.ValidateCommand;
import nl.knaw.dans.ingest.core.service.exception.RejectedDepositException;
import nl.knaw.dans.ingest.core.service.mapping.Amd;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class DepositMigrationTask extends DepositIngestTask {
    public DepositMigrationTask(DepositToDvDatasetMetadataMapper datasetMetadataMapper, Deposit deposit, DataverseClient dataverseClient, String depositorRole, Pattern fileExclusionPattern,
        ZipFileHandler zipFileHandler, Map<String, String> variantToLicense, List<URI> supportedLicenses, DansBagValidator dansBagValidator, int publishAwaitUnlockMillisecondsBetweenRetries,
        int publishAwaitUnlockMaxNumberOfRetries, Path outboxDir, EventWriter eventWriter, DepositManager depositManager) {

        super(
            datasetMetadataMapper, deposit, dataverseClient, depositorRole, fileExclusionPattern, zipFileHandler, variantToLicense, supportedLicenses, dansBagValidator,
            publishAwaitUnlockMillisecondsBetweenRetries, publishAwaitUnlockMaxNumberOfRetries, outboxDir, eventWriter, depositManager);
    }

    @Override
    void checkDepositType() {
        var deposit = getDeposit();

        if (StringUtils.isEmpty(deposit.getDoi())) {
            throw new IllegalArgumentException("Deposit for migrated dataset MUST have deposit property identifier.doi set");
        }

        validateVaultMetadata(deposit.getVaultMetadata());
    }

    void validateVaultMetadata(VaultMetadata vaultMetadata) {
        var missing = new ArrayList<String>();

        if (StringUtils.isBlank(vaultMetadata.getPid())) {
            missing.add("dataversePid");
        }

        if (StringUtils.isBlank(vaultMetadata.getNbn())) {
            missing.add("dataverseNbn");
        }

        if (!missing.isEmpty()) {
            var msg = String.join(", ", missing);
            throw new RuntimeException(String.format(
                "Not enough Data Vault Metadata for import deposit, missing: %s", msg
            ));
        }
    }

    @Override
    DatasetEditor newDatasetCreator(Dataset dataset, String depositorRole) {
        return newDatasetCreator(dataset, depositorRole, true);
    }

    @Override
    DatasetEditor newDatasetUpdater(Dataset dataset) {
        return newDatasetUpdater(dataset, true);
    }

    @Override
    void checkPersonalDataPresent(Document document) {
        if (document == null) {
            throw new RejectedDepositException(getDeposit(), "Migration deposit MUST have an agreements.xml");
        }
    }

    @Override
    Optional<String> getDateOfDeposit() {
        return Optional.ofNullable(getDeposit().getAmd())
            .map(Amd::toDateOfDeposit)
            .flatMap(i -> i);
    }

    @Override
    void publishDataset(String persistentId) throws Exception {
        try {
            var deposit = getDeposit();
            var amd = deposit.getAmd();

            if (amd == null) {
                throw new Exception(String.format("no AMD found for %s", persistentId));
            }

            var date = Amd.toPublicationDate(amd);

            if (date.isEmpty()) {
                throw new IllegalArgumentException(String.format("no publication date found in AMD for %s", persistentId));
            }

            var dataset = dataverseClient.dataset(persistentId);

            dataset.releaseMigrated(date.get(), true);
            dataset.awaitUnlock(publishAwaitUnlockMaxNumberOfRetries, publishAwaitUnlockMillisecondsBetweenRetries);
        }
        catch (IOException | DataverseException e) {
            log.error("Unable to publish dataset", e);
        }
    }

    @Override
    void postPublication(String persistentId) {
        // do nothing
    }

    void validateDeposit() {
        var deposit = getDeposit();

        if (dansBagValidator != null) {
            var result = dansBagValidator.validateBag(
                deposit.getBagDir(), ValidateCommand.PackageTypeEnum.MIGRATION, 1);

            if (!result.getIsCompliant()) {
                var violations = result.getRuleViolations().stream()
                    .map(r -> String.format("- [%s] %s", r.getRule(), r.getViolation()))
                    .collect(Collectors.joining("\n"));

                throw new RejectedDepositException(deposit, String.format(
                    "Bag was not valid according to Profile Version %s. Violations: %s",
                    result.getProfileVersion(), violations)
                );
            }
        }
    }

}

