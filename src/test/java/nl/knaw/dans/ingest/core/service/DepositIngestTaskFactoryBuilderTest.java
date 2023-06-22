package nl.knaw.dans.ingest.core.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import nl.knaw.dans.ingest.DdIngestFlowConfiguration;
import nl.knaw.dans.ingest.core.config.IngestAreaConfig;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class DepositIngestTaskFactoryBuilderTest {

    @Test
    public void debug_etc_does_not_throw() throws Exception {
        final var mapper = Jackson.newObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        final var factory = new YamlConfigurationFactory<>(DdIngestFlowConfiguration.class, Validators.newValidator(), mapper, "dw");
        final var dir = "src/test/resources/debug-etc";
        final var config = factory.build(FileInputStream::new, dir + "/config.yml");
        config.getIngestFlow().setMappingDefsDir(Paths.get(dir));
        IngestAreaConfig areaConfig = config.getIngestFlow().getAutoIngest();

        assertDoesNotThrow(() -> new DepositIngestTaskFactoryBuilder(config, null, null)
            .createTaskFactory(areaConfig, null, false));
    }
}
