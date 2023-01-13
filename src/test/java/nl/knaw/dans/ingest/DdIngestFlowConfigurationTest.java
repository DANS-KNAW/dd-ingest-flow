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
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DdIngestFlowConfigurationTest {

    private final YamlConfigurationFactory<DdIngestFlowConfiguration> factory;

    {
        ObjectMapper mapper = Jackson.newObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        factory = new YamlConfigurationFactory<>(DdIngestFlowConfiguration.class, Validators.newValidator(), mapper, "dw");
    }

    @Test
    public void canReadAssemblyWithSomeDefaultRoles() throws IOException, ConfigurationException {
        var config = factory.build(FileInputStream::new, "src/main/assembly/dist/cfg/config.yml");
        assertEquals("swordupdater", config.getIngestFlow().getAutoIngest().getDepositorRole());
        assertEquals("contributorplus", config.getIngestFlow().getImportConfig().getDepositorRole());
        assertEquals("contributorplus", config.getIngestFlow().getMigration().getDepositorRole());
    }

    @Test
    public void canReadTestWithOnlyDefaultRole() throws IOException, ConfigurationException {
        var config = factory.build(new ResourceConfigurationSourceProvider(), "debug-etc/config.yml");
        assertEquals("contributor", config.getIngestFlow().getAutoIngest().getDepositorRole());
        assertEquals("contributor", config.getIngestFlow().getMigration().getDepositorRole());
        assertEquals("contributor", config.getIngestFlow().getImportConfig().getDepositorRole());
    }

    @Test
    public void canReadAllCustomRoles() throws IOException, ConfigurationException {
        var s = FileUtils.readFileToString(new File("src/test/resources/debug-etc/config.yml"), "UTF8")
            .replace("depositorRole: contributor", "depositorRole: contributorplus")
            .replace("autoIngest:", "autoIngest:\n    depositorRole: swordupdater")
            .replace("migration:", "migration:\n    depositorRole: depositor")
            .replace("import:", "import:\n    depositorRole: contributor");
        File testFile = new File("target/test/" + this.getClass().getSimpleName() + "/config.yml");
        FileUtils.write(testFile, s, "UTF8");
        var config = factory.build(FileInputStream::new, testFile.toString());
        assertEquals("swordupdater", config.getIngestFlow().getAutoIngest().getDepositorRole());
        assertEquals("depositor", config.getIngestFlow().getMigration().getDepositorRole());
        assertEquals("contributor", config.getIngestFlow().getImportConfig().getDepositorRole());
    }
}
