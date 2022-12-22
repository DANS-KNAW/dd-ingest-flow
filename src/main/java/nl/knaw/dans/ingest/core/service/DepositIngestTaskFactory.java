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
import java.nio.file.Files;
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

    public DepositIngestTaskFactory(
        boolean isMigration,
        DataverseClient dataverseClient,
        DansBagValidator dansBagValidator,
        IngestFlowConfig ingestFlowConfig,
        DataverseExtra dataverseExtra,
        DepositManager depositManager,
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

    public DepositIngestTask createIngestTask(Path depositDir, Path outboxDir, EventWriter eventWriter) throws InvalidDepositException, IOException {
        try {
            var deposit = depositManager.loadDeposit(depositDir);
            return createDepositIngestTask(deposit, outboxDir, eventWriter);
        }
        catch (InvalidDepositException | IOException e) {
            // the reading of the deposit failed, so we cannot update its internal state. All we can do is move it
            // to the "failed" directory
            log.error("Unable to load deposit properties, considering deposit at path {} to be broken", depositDir);
            moveDepositToOutbox(depositDir, outboxDir);
            throw e;
        }
        catch (Throwable e) {
            // if something bad happens while loading the deposit, we want to wrap it into an InvalidDepositException as well
            log.error("Unexpected error occurred while loading deposit at path {}, moving deposit", depositDir);
            moveDepositToOutbox(depositDir, outboxDir);
            throw new InvalidDepositException("Unexpected error occurred: " + e.getMessage(), e);
        }
    }

    void moveDepositToOutbox(Path depositDir, Path outboxDir) throws IOException {
        var target = outboxDir
            .resolve(OutboxSubDir.FAILED.getValue())
            .resolve(depositDir.getFileName());

        log.info("Moving path {} to {}", depositDir, target);
        Files.move(depositDir, target);
    }

    private DepositIngestTask createDepositIngestTask(Deposit deposit, Path outboxDir, EventWriter eventWriter) {
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
}
