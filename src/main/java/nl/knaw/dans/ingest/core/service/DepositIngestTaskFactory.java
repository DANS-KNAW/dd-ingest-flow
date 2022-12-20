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
import nl.knaw.dans.ingest.core.config.DataverseExtra;
import nl.knaw.dans.ingest.core.config.IngestFlowConfig;
import nl.knaw.dans.ingest.core.service.exception.InvalidDepositException;
import nl.knaw.dans.ingest.core.service.mapper.DepositToDvDatasetMetadataMapperFactory;
import nl.knaw.dans.lib.dataverse.DataverseClient;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Pattern;

@Slf4j
public class DepositIngestTaskFactory {

    private final DataverseClient dataverseClient;
    private final DansBagValidator dansBagValidator;

    private final IngestFlowConfig ingestFlowConfig;
    private final DataverseExtra dataverseExtra;
    private final DepositManager depositManager;

    private final boolean isMigration;
    private final DepositToDvDatasetMetadataMapperFactory depositToDvDatasetMetadataMapperFactory;
    private final ZipFileHandler zipFileHandler;

    public DepositIngestTaskFactory(boolean isMigration, DataverseClient dataverseClient,
        DansBagValidator dansBagValidator, IngestFlowConfig ingestFlowConfig,
        DataverseExtra dataverseExtra, DepositManager depositManager,
        DepositToDvDatasetMetadataMapperFactory depositToDvDatasetMetadataMapperFactory,
        ZipFileHandler zipFileHandler
    ) throws IOException, URISyntaxException {
        this.isMigration = isMigration;
        this.dataverseClient = dataverseClient;
        this.dansBagValidator = dansBagValidator;
        this.ingestFlowConfig = ingestFlowConfig;
        this.dataverseExtra = dataverseExtra;
        this.depositManager = depositManager;
        this.depositToDvDatasetMetadataMapperFactory = depositToDvDatasetMetadataMapperFactory;
        this.zipFileHandler = zipFileHandler;
    }

    public DepositIngestTask createDepositIngestTask(Deposit deposit, Path outboxDir, EventWriter eventWriter) {
        var fileExclusionPattern = Optional.ofNullable(ingestFlowConfig.getFileExclusionPattern())
            .map(Pattern::compile)
            .orElse(null);

        log.info("Creating deposit ingest task, isMigration = {}", this.isMigration);
        if (this.isMigration) {
            return new DepositMigrationTask(
                depositToDvDatasetMetadataMapperFactory,
                deposit,
                dataverseClient,
                ingestFlowConfig.getDepositorRole(),
                fileExclusionPattern,
                zipFileHandler,
                ingestFlowConfig.getVariantToLicense(),
                ingestFlowConfig.getSupportedLicenses(),
                dansBagValidator,
                dataverseExtra.getPublishAwaitUnlockMaxRetries(),
                dataverseExtra.getPublishAwaitUnlockWaitTimeMs(),
                outboxDir,
                eventWriter,
                depositManager
            );
        }
        else {
            return new DepositIngestTask(
                depositToDvDatasetMetadataMapperFactory,
                deposit,
                dataverseClient,
                ingestFlowConfig.getDepositorRole(),
                fileExclusionPattern,
                zipFileHandler,
                ingestFlowConfig.getVariantToLicense(),
                ingestFlowConfig.getSupportedLicenses(),
                dansBagValidator,
                dataverseExtra.getPublishAwaitUnlockMaxRetries(),
                dataverseExtra.getPublishAwaitUnlockWaitTimeMs(),
                outboxDir,
                eventWriter,
                depositManager);
        }

    }

    public DepositIngestTask createIngestTask(Path depositDir, Path outboxDir, EventWriter eventWriter) {
        try {
            var deposit = depositManager.loadDeposit(depositDir);
            return createDepositIngestTask(deposit, outboxDir, eventWriter);
        }
        catch (InvalidDepositException | IOException e) {
            throw new RuntimeException("Invalid deposit", e);
        }
    }
}
