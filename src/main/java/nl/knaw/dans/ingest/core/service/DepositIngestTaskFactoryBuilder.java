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

import nl.knaw.dans.ingest.core.config.DataverseExtra;
import nl.knaw.dans.ingest.core.config.IngestFlowConfig;
import nl.knaw.dans.ingest.core.deposit.DepositManager;
import nl.knaw.dans.ingest.core.deposit.DepositReader;
import nl.knaw.dans.ingest.core.service.mapper.DepositToDvDatasetMetadataMapperFactory;
import nl.knaw.dans.lib.dataverse.DataverseClient;

import java.io.IOException;
import java.net.URISyntaxException;

public class DepositIngestTaskFactoryBuilder {
    private final DataverseClient dataverseClient;
    private final DansBagValidator dansBagValidator;
    private final IngestFlowConfig ingestFlowConfig;
    private final DataverseExtra dataverseExtra;
    private final DepositManager depositManager;
    private final DepositToDvDatasetMetadataMapperFactory depositToDvDatasetMetadataMapperFactory;
    private final ZipFileHandler zipFileHandler;
    private final BlockedTargetService blockedTargetService;
    private final DepositReader depositReader;

    public DepositIngestTaskFactoryBuilder(
        DataverseClient dataverseClient,
        DansBagValidator dansBagValidator,
        IngestFlowConfig ingestFlowConfig,
        DataverseExtra dataverseExtra,
        DepositManager depositManager,
        DepositToDvDatasetMetadataMapperFactory depositToDvDatasetMetadataMapperFactory,
        ZipFileHandler zipFileHandler,
        BlockedTargetService blockedTargetService,
        DepositReader depositReader) {
        this.dataverseClient = dataverseClient;
        this.dansBagValidator = dansBagValidator;
        this.ingestFlowConfig = ingestFlowConfig;
        this.dataverseExtra = dataverseExtra;
        this.depositManager = depositManager;
        this.depositToDvDatasetMetadataMapperFactory = depositToDvDatasetMetadataMapperFactory;
        this.zipFileHandler = zipFileHandler;
        this.blockedTargetService = blockedTargetService;
        this.depositReader = depositReader;
    }

    public DepositIngestTaskFactory createTaskFactory(boolean isMigration, String depositorRole) throws IOException, URISyntaxException {
        return new DepositIngestTaskFactory(
            isMigration,
            depositorRole,
            dataverseClient,
            dansBagValidator,
            ingestFlowConfig,
            dataverseExtra,
            depositManager,
            depositToDvDatasetMetadataMapperFactory,
            zipFileHandler,
            blockedTargetService
        );
    }
}
