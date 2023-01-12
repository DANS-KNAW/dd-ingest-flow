package nl.knaw.dans.ingest.core.service;

import nl.knaw.dans.ingest.core.config.DataverseExtra;
import nl.knaw.dans.ingest.core.config.IngestFlowConfig;
import nl.knaw.dans.ingest.core.service.mapper.DepositToDvDatasetMetadataMapperFactory;
import nl.knaw.dans.lib.dataverse.DataverseClient;

public class DepositIngestTaskParams {
    private final DataverseClient dataverseClient;
    private final DansBagValidator dansBagValidator;
    private final IngestFlowConfig ingestFlowConfig;
    private final DataverseExtra dataverseExtra;
    private final DepositManager depositManager;
    private final DepositToDvDatasetMetadataMapperFactory depositToDvDatasetMetadataMapperFactory;
    private final ZipFileHandler zipFileHandler;

    public DepositIngestTaskParams(
        DataverseClient dataverseClient,
        DansBagValidator dansBagValidator,
        IngestFlowConfig ingestFlowConfig,
        DataverseExtra dataverseExtra,
        DepositManager depositManager,
        DepositToDvDatasetMetadataMapperFactory depositToDvDatasetMetadataMapperFactory,
        ZipFileHandler zipFileHandler
    ) {
        this.dataverseClient = dataverseClient;
        this.dansBagValidator = dansBagValidator;
        this.ingestFlowConfig = ingestFlowConfig;
        this.dataverseExtra = dataverseExtra;
        this.depositManager = depositManager;
        this.depositToDvDatasetMetadataMapperFactory = depositToDvDatasetMetadataMapperFactory;
        this.zipFileHandler = zipFileHandler;
    }

    public DataverseClient getDataverseClient() {
        return dataverseClient;
    }

    public DansBagValidator getDansBagValidator() {
        return dansBagValidator;
    }

    public IngestFlowConfig getIngestFlowConfig() {
        return ingestFlowConfig;
    }

    public DataverseExtra getDataverseExtra() {
        return dataverseExtra;
    }

    public DepositManager getDepositManager() {
        return depositManager;
    }

    public DepositToDvDatasetMetadataMapperFactory getDepositToDvDatasetMetadataMapperFactory() {
        return depositToDvDatasetMetadataMapperFactory;
    }

    public ZipFileHandler getZipFileHandler() {
        return zipFileHandler;
    }
}
