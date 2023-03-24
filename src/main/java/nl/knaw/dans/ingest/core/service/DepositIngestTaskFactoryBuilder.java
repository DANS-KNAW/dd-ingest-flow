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

import nl.knaw.dans.ingest.core.config.IngestFlowConfig;
import nl.knaw.dans.ingest.core.dataverse.DatasetService;
import nl.knaw.dans.ingest.core.deposit.DepositManager;
import nl.knaw.dans.ingest.core.service.mapper.DepositToDvDatasetMetadataMapperFactory;
import nl.knaw.dans.ingest.core.validation.DepositorAuthorizationValidator;

import java.io.IOException;
import java.net.URISyntaxException;

public class DepositIngestTaskFactoryBuilder {
    private final DansBagValidator dansBagValidator;
    private final IngestFlowConfig ingestFlowConfig;
    private final DepositManager depositManager;
    private final DepositToDvDatasetMetadataMapperFactory depositToDvDatasetMetadataMapperFactory;
    private final ZipFileHandler zipFileHandler;
    private final DatasetService datasetService;
    private final BlockedTargetService blockedTargetService;

    public DepositIngestTaskFactoryBuilder(
        DansBagValidator dansBagValidator,
        IngestFlowConfig ingestFlowConfig,
        DepositManager depositManager,
        DepositToDvDatasetMetadataMapperFactory depositToDvDatasetMetadataMapperFactory,
        ZipFileHandler zipFileHandler,
        DatasetService datasetService,
        BlockedTargetService blockedTargetService) {
        this.dansBagValidator = dansBagValidator;
        this.ingestFlowConfig = ingestFlowConfig;
        this.depositManager = depositManager;
        this.depositToDvDatasetMetadataMapperFactory = depositToDvDatasetMetadataMapperFactory;
        this.zipFileHandler = zipFileHandler;
        this.datasetService = datasetService;
        this.blockedTargetService = blockedTargetService;
    }

    // TODO this should really be refactored so that we don't need a task factory builder, we just create ingest task factories for each importarea with the proper config
    public DepositIngestTaskFactory createTaskFactory(boolean isMigration, String depositorRole, DepositorAuthorizationValidator depositorAuthorizationValidator) throws IOException, URISyntaxException {
        return new DepositIngestTaskFactory(
            isMigration,
            depositorRole,
            dansBagValidator,
            ingestFlowConfig,
            depositManager,
            depositToDvDatasetMetadataMapperFactory,
            zipFileHandler,
            datasetService,
            blockedTargetService,
            depositorAuthorizationValidator
        );
    }
}
