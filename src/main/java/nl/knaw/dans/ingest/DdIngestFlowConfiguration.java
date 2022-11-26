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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.db.DataSourceFactory;
import nl.knaw.dans.ingest.core.config.DataverseExtra;
import nl.knaw.dans.ingest.core.config.IngestFlowConfig;
import nl.knaw.dans.ingest.core.config.ValidateDansBagConfig;
import nl.knaw.dans.lib.util.DataverseClientFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class DdIngestFlowConfiguration extends Configuration {

    private IngestFlowConfig ingestFlow;

    private DataverseClientFactory dataverse;

    private DataverseExtra dataverseExtra;

    private ValidateDansBagConfig validateDansBag;
    private DataSourceFactory taskEventDatabase;

    @Valid
    @NotNull
    private JerseyClientConfiguration dansBagValidatorClient = new JerseyClientConfiguration();

    @JsonProperty("dansBagValidatorClient")
    public JerseyClientConfiguration getDansBagValidatorClient() {
        return dansBagValidatorClient;
    }

    @JsonProperty("dansBagValidatorClient")
    public void setDansBagValidatorClient(JerseyClientConfiguration jerseyClient) {
        this.dansBagValidatorClient = jerseyClient;
    }

    public IngestFlowConfig getIngestFlow() {
        return ingestFlow;
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
