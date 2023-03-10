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
import nl.knaw.dans.ingest.core.dataverse.DatasetService;
import nl.knaw.dans.ingest.core.deposit.DepositManager;
import nl.knaw.dans.ingest.core.domain.Deposit;
import nl.knaw.dans.ingest.core.domain.DepositLocation;
import nl.knaw.dans.ingest.core.domain.VaultMetadata;
import nl.knaw.dans.ingest.core.exception.RejectedDepositException;
import nl.knaw.dans.ingest.core.service.mapper.DepositToDvDatasetMetadataMapperFactory;
import nl.knaw.dans.ingest.core.service.mapper.mapping.Amd;
import nl.knaw.dans.ingest.core.validation.DepositorAuthorizationValidator;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.validatedansbag.api.ValidateCommand;
import org.apache.commons.lang3.StringUtils;

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
    public DepositMigrationTask(
        DepositToDvDatasetMetadataMapperFactory datasetMetadataMapperFactory,
        DepositLocation depositLocation,
        String depositorRole,
        Pattern fileExclusionPattern,
        ZipFileHandler zipFileHandler,
        List<URI> supportedLicenses,
        DansBagValidator dansBagValidator,
        Path outboxDir,
        EventWriter eventWriter,
        DepositManager depositManager,
        DatasetService datasetService,
        BlockedTargetService blockedTargetService,
        DepositorAuthorizationValidator depositorAuthorizationValidator
    ) {
        super(
            datasetMetadataMapperFactory, depositLocation, depositorRole, fileExclusionPattern, zipFileHandler, supportedLicenses,
            dansBagValidator,
            outboxDir, eventWriter, depositManager, datasetService, blockedTargetService, depositorAuthorizationValidator);
    }

    @Override
    void checkDepositType() {
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
    Optional<String> getDateOfDeposit() {
        return Optional.ofNullable(deposit.getAmd())
            .map(Amd::toDateOfDeposit)
            .flatMap(i -> i);
    }

    @Override
    void publishDataset(String persistentId) throws IOException, DataverseException {
        var amd = deposit.getAmd();

        if (amd == null) {
            throw new RuntimeException(String.format("no AMD found for %s", persistentId));
        }

        var date = Amd.toPublicationDate(amd);

        if (date.isEmpty()) {
            throw new IllegalArgumentException(String.format("no publication date found in AMD for %s", persistentId));
        }

        datasetService.releaseMigrated(persistentId, date.get());
    }

    @Override
    void postPublication(String persistentId) {
        // do nothing
    }

    void validateDeposit() {
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

    @Override
    String resolveDoi(Deposit deposit) throws IOException, DataverseException {
        return getDoi("dansBagId", deposit.getIsVersionOf());
    }
}

