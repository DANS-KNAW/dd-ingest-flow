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

package nl.knaw.dans.ingest;

import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import nl.knaw.dans.ingest.core.config.DataverseExtra;
import nl.knaw.dans.ingest.core.config.IngestAreaConfig;
import nl.knaw.dans.ingest.core.config.IngestFlowConfig;
import nl.knaw.dans.ingest.core.config.ValidateDansBagConfig;
import nl.knaw.dans.lib.util.DataverseClientFactory;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

public class DdIngestFlowConfiguration extends Configuration {

    @NotNull
    private IngestFlowConfig ingestFlow;

    @NotNull
    private DataverseClientFactory dataverse;

    @NotNull
    private DataverseExtra dataverseExtra;

    @NotNull
    private ValidateDansBagConfig validateDansBag;

    @NotNull
    private DataSourceFactory taskEventDatabase;

    public IngestFlowConfig getIngestFlow() {
        applyDefaults(ingestFlow.getAutoIngest());
        applyDefaults(ingestFlow.getImportConfig());
        applyDefaults(ingestFlow.getMigration());
        return ingestFlow;
    }

    private void applyDefaults(IngestAreaConfig ingestAreaConfig) {
        if (StringUtils.isBlank(ingestAreaConfig.getDepositorRole())) {
            ingestAreaConfig.setDepositorRole(ingestFlow.getDepositorRole());
        }
        var defaultAuthorization = ingestFlow.getAuthorization();
        if (ingestAreaConfig.getAuthorization() == null) {
            ingestAreaConfig.setAuthorization(defaultAuthorization);
        } else {
            var authorization = ingestAreaConfig.getAuthorization();
            if (StringUtils.isBlank(authorization.getDatasetCreator())) {
                authorization.setDatasetCreator(defaultAuthorization.getDatasetCreator());
            }
            if (StringUtils.isBlank(authorization.getDatasetUpdater())) {
                authorization.setDatasetUpdater(defaultAuthorization.getDatasetUpdater());
            }
        }
        if (StringUtils.isBlank(ingestAreaConfig.getApiKey())) {
            ingestAreaConfig.setApiKey(dataverse.getApiKey());
        }
    }

    public void setIngestFlow(IngestFlowConfig ingestFlow) {
        this.ingestFlow = ingestFlow;
    }

    public DataverseClientFactory getDataverse() {
        return dataverse;
    }

    public void setDataverse(DataverseClientFactory dataverse) {
        this.dataverse = dataverse;
    }

    public DataverseExtra getDataverseExtra() {
        return dataverseExtra;
    }

    public void setDataverseExtra(DataverseExtra dataverseExtra) {
        this.dataverseExtra = dataverseExtra;
    }

    public ValidateDansBagConfig getValidateDansBag() {
        return validateDansBag;
    }

    public void setValidateDansBag(ValidateDansBagConfig validateDansBag) {
        this.validateDansBag = validateDansBag;
    }

    public DataSourceFactory getTaskEventDatabase() {
        return taskEventDatabase;
    }

    public void setTaskEventDatabase(DataSourceFactory dataSourceFactory) {
        this.taskEventDatabase = dataSourceFactory;
    }

}
