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

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import nl.knaw.dans.ingest.DdIngestFlowConfiguration;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class DepositIngestTaskFactoryBuilderTest {

    @Test
    public void debug_etc_does_not_throw() throws Exception {
        final var mapper = Jackson.newObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        final var factory = new YamlConfigurationFactory<>(DdIngestFlowConfiguration.class, Validators.newValidator(), mapper, "dw");
        final var dir = "src/test/resources/debug-etc";
        final var config = factory.build(FileInputStream::new, dir + "/config.yml");
        final var areaConfig = config.getIngestFlow().getAutoIngest();

        assertDoesNotThrow(() -> new DepositIngestTaskFactoryBuilder(config, null, null)
            .createTaskFactory(areaConfig, false));
    }
}
