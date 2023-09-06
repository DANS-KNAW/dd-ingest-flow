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

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static nl.knaw.dans.ingest.IngestFlowConfigReader.readIngestFlowConfiguration;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class ConfigurationTest {

    private final String testDir = "target/test/" + this.getClass().getSimpleName();
    private final YamlConfigurationFactory<DdIngestFlowConfiguration> factory;

    {
        final var mapper = Jackson.newObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        factory = new YamlConfigurationFactory<>(DdIngestFlowConfiguration.class, Validators.newValidator(), mapper, "dw");
    }

    @Test
    public void assembly_dist_cfg_does_not_throw() {
        final var dir = "src/main/assembly/dist/cfg";
        final var config = assertDoesNotThrow(() -> factory.build(FileInputStream::new, dir + "/config.yml"));
        config.getIngestFlow().setMappingDefsDir(Paths.get(dir));
        assertDoesNotThrow(() -> readIngestFlowConfiguration(config.getIngestFlow()));
    }

    @Test
    public void debug_etc_does_not_throw() {
        final var dir = "src/test/resources/debug-etc";
        final var config = assertDoesNotThrow(() -> factory.build(FileInputStream::new, dir + "/config.yml"));
        config.getIngestFlow().setMappingDefsDir(Paths.get(dir));
        assertDoesNotThrow(() -> readIngestFlowConfiguration(config.getIngestFlow()));
    }

    @Test
    public void unit_test_config_yml_produces_custom_values() throws IOException, ConfigurationException {
        final var ingestFlowConfig = factory.build(new ResourceConfigurationSourceProvider(), "unit-test-config.yml").getIngestFlow();

        assertThat(ingestFlowConfig.getAutoIngest().getDepositorRole()).isEqualTo("swordupdater");
        assertThat(ingestFlowConfig.getMigration().getDepositorRole()).isEqualTo("migrator");
        assertThat(ingestFlowConfig.getImportConfig().getDepositorRole()).isEqualTo("importer");

        assertThat(ingestFlowConfig.getImportConfig().getApiKey()).isEqualTo("changeme1");
        assertThat(ingestFlowConfig.getAutoIngest().getApiKey()).isEqualTo("changeme3");
        assertThat(ingestFlowConfig.getMigration().getApiKey()).isEqualTo("changeme2");

        assertThat(ingestFlowConfig.getMigration().getAuthorization().getDatasetCreator()).isEqualTo("migrationcreator");
        assertThat(ingestFlowConfig.getImportConfig().getAuthorization().getDatasetUpdater()).isEqualTo("importupdater");

        assertThat(ingestFlowConfig.getVaultMetadataKey()).isEqualTo("somesecret");
    }

    @Test
    public void amended_unit_test_config_yml_produces_default_values() throws IOException, ConfigurationException {

        final var linesToRemove = "^.* # custom value\n";
        final var remainingLines = Pattern.compile(linesToRemove, Pattern.MULTILINE)
            .matcher(readFileToString(new File("src/test/resources/unit-test-config.yml"), UTF_8))
            .replaceAll("");
        final var testFile = new File(testDir + "/config.yml");
        FileUtils.write(testFile, remainingLines, UTF_8);

        final var ingestFlowConfig = factory.build(FileInputStream::new, testFile.toString()).getIngestFlow();

        assertThat(ingestFlowConfig.getMigration().getAuthorization().getDatasetUpdater()).isEqualTo("contributorplus");
        assertThat(ingestFlowConfig.getImportConfig().getAuthorization().getDatasetCreator()).isEqualTo("dsContributor");
        assertThat(ingestFlowConfig.getAutoIngest().getAuthorization().getDatasetUpdater()).isEqualTo("contributorplus");
        assertThat(ingestFlowConfig.getAutoIngest().getAuthorization().getDatasetCreator()).isEqualTo("dsContributor");

        assertThat(ingestFlowConfig.getImportConfig().getDepositorRole()).isEqualTo("contributor");
        assertThat(ingestFlowConfig.getAutoIngest().getDepositorRole()).isEqualTo("contributor");
        assertThat(ingestFlowConfig.getMigration().getDepositorRole()).isEqualTo("contributor");

        assertThat(ingestFlowConfig.getImportConfig().getApiKey()).isEqualTo("changeme4");
        assertThat(ingestFlowConfig.getAutoIngest().getApiKey()).isEqualTo("changeme4");
        assertThat(ingestFlowConfig.getMigration().getApiKey()).isEqualTo("changeme4");
    }
}
