package nl.knaw.dans.ingest.core;

import nl.knaw.dans.ingest.core.config.IngestAreaConfig;
import nl.knaw.dans.ingest.core.config.IngestFlowConfig;

public enum IngestType {
    MIGRATION, IMPORT, AUTO_INGEST;

    public IngestAreaConfig getIngestAreaConfig(IngestFlowConfig ingestFlowConfig) {
        switch (this) {
            case MIGRATION:
                return ingestFlowConfig.getMigration();
            case AUTO_INGEST:
                return ingestFlowConfig.getAutoIngest();
            case IMPORT:
                return ingestFlowConfig.getImportConfig();
            default:
                throw new IllegalStateException("depositor rol not known for " + this);
        }
    }
}
