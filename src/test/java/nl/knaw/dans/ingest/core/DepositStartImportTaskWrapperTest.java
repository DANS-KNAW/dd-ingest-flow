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
package nl.knaw.dans.ingest.core;

import nl.knaw.dans.ingest.core.dataverse.DatasetService;
import nl.knaw.dans.ingest.core.deposit.BagDirResolverImpl;
import nl.knaw.dans.ingest.core.deposit.DepositFileLister;
import nl.knaw.dans.ingest.core.deposit.DepositLocationReaderImpl;
import nl.knaw.dans.ingest.core.deposit.DepositManagerImpl;
import nl.knaw.dans.ingest.core.deposit.DepositReaderImpl;
import nl.knaw.dans.ingest.core.deposit.DepositWriterImpl;
import nl.knaw.dans.ingest.core.domain.DepositLocation;
import nl.knaw.dans.ingest.core.io.BagDataManager;
import nl.knaw.dans.ingest.core.io.FileService;
import nl.knaw.dans.ingest.core.service.BlockedTargetService;
import nl.knaw.dans.ingest.core.service.DansBagValidator;
import nl.knaw.dans.ingest.core.service.DepositIngestTask;
import nl.knaw.dans.ingest.core.service.DepositMigrationTask;
import nl.knaw.dans.ingest.core.service.EventWriter;
import nl.knaw.dans.ingest.core.service.ManifestHelper;
import nl.knaw.dans.ingest.core.service.XmlReader;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import nl.knaw.dans.ingest.core.service.ZipFileHandler;
import nl.knaw.dans.ingest.core.service.mapper.DepositToDvDatasetMetadataMapperFactory;
import nl.knaw.dans.ingest.core.validation.DepositorAuthorizationValidatorImpl;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DepositStartImportTaskWrapperTest {
    private static final Path testDepositsBasedir = Paths.get("src/test/resources/unordered-stub-deposits/");

    /*
        deposit2_first  Created: 2020-02-15T09:04:00.345+03:00  = 2020-02-15T06:04:00.345+00:00

        deposit1_first   Created: 2020-02-15T09:01:00.345+01:00 = 2020-02-15T08:01:00.345+00:00
        deposit1_a       Created: 2020-02-15T09:02:00.345+01:00 = 2020-02-15T08:02:00.345+00:00
        deposit1_b       Created: 2020-02-15T09:03:00.345+01:00 = 2020-02-15T08:03:00.345+00:00

        deposit2_a      Created: 2020-02-15T11:04:00.345+03:00 = 2020-02-15T08:04:00.345+00:00

        deposit3_notimezone  Created: 2020-02-15T09:00:00.123 -> ERROR no time zone
        deposit3_nocreated     -> ERROR no created timestamp
        deposit3_2created     -> ERROR 2 created timestamps

     */
    private final XmlReader xmlReader = new XmlReaderImpl();

    private final Map<String, String> iso1ToDataverseLanguage = new HashMap<>();
    private final Map<String, String> iso2ToDataverseLanguage = new HashMap<>();

    private DepositIngestTask createTaskWrapper(String depositName, String created) {
        var mapper = getMapperFactory();
        var validator = Mockito.mock(DansBagValidator.class);
        var depositFileLister = Mockito.mock(DepositFileLister.class);
        var eventWriter = Mockito.mock(EventWriter.class);
        var blockedTargetService = Mockito.mock(BlockedTargetService.class);
        var fileService = Mockito.mock(FileService.class);
        var bagDataManager = Mockito.mock(BagDataManager.class);
        var bagDirResolver = new BagDirResolverImpl(fileService);
        var depositLocationReader = new DepositLocationReaderImpl(bagDirResolver, bagDataManager);
        var manifestHelper = Mockito.mock(ManifestHelper.class);
        var depositReader = new DepositReaderImpl(xmlReader, bagDirResolver, fileService, bagDataManager, depositFileLister, manifestHelper);
        var depositWriter = new DepositWriterImpl(bagDataManager);
        var depositManager = new DepositManagerImpl(depositReader, depositLocationReader, depositWriter);
        // TODO dont actually read the data from disk, just keep it in this class
        //        var depositLocation = depositLocationReader.readDepositLocation(testDepositsBasedir.resolve(depositName));
        var date = OffsetDateTime.parse(created);
        var depositLocation = new DepositLocation(testDepositsBasedir.resolve(depositName), depositName, UUID.randomUUID().toString(), date);
        var datasetService = Mockito.mock(DatasetService.class);
        var depositorAuthorizationValidator = Mockito.mock(DepositorAuthorizationValidatorImpl.class);

        return new DepositMigrationTask(
            mapper,
            depositLocation,
            "dummy",
            null,
            new ZipFileHandler(Path.of("target/test")),
            List.of(),
            validator,
            Path.of("dummy"),
            eventWriter,
            depositManager,
            datasetService,
            blockedTargetService,
            depositorAuthorizationValidator
        );
    }

    @BeforeEach
    void setUp() {
        iso1ToDataverseLanguage.put("nl", "Dutch");
        iso1ToDataverseLanguage.put("de", "German");

        iso2ToDataverseLanguage.put("dut", "Dutch");
        iso2ToDataverseLanguage.put("ger", "German");
    }

    DepositToDvDatasetMetadataMapperFactory getMapperFactory() {
        return new DepositToDvDatasetMetadataMapperFactory(
            iso1ToDataverseLanguage, iso2ToDataverseLanguage,
            List.of("Netherlands", "United Kingdom", "Belgium", "Germany"),
            Map.of(),
            Mockito.mock(DataverseClient.class)
        );
    }

    @Test
    void deposits_should_be_ordered_by_created_timestamp() {
        List<DepositIngestTask> sorted = Stream.of(
            createTaskWrapper("deposit2_a", "2020-02-15T11:04:00.345+03:00"),
            createTaskWrapper("deposit1_b", "2020-02-15T09:03:00.345+01:00"),
            createTaskWrapper("deposit1_a", "2020-02-15T09:02:00.345+01:00"),
            createTaskWrapper("deposit1_first", "2020-02-15T09:01:00.345+01:00"),
            createTaskWrapper("deposit2_first", "2020-02-15T09:03:00.345+03:00")
        ).sorted().collect(Collectors.toList());

        assertEquals("deposit2_first", sorted.get(0).getTarget());
        assertEquals("deposit1_first", sorted.get(1).getTarget());
        assertEquals("deposit1_a", sorted.get(2).getTarget());
        assertEquals("deposit1_b", sorted.get(3).getTarget());
        assertEquals("deposit2_a", sorted.get(4).getTarget());
    }

    //    @Test
    //    void fail_fast_if_no_time_zone_in_created_timestamp() {
    //        var thrown = assertThrows(InvalidDepositException.class,
    //            () -> createTaskWrapper("deposit3_notimezone"));
    //
    //        assertEquals(thrown.getCause().getClass(), DateTimeParseException.class);
    //    }
    //
    //    @Test
    //    void fail_fast_if_no_created_timestamp() {
    //        var thrown = assertThrows(InvalidDepositException.class,
    //            () -> createTaskWrapper("deposit3_nocreated"));
    //        assertTrue(thrown.getMessage().contains("Missing 'created' property in bag-info.txt"));
    //    }
    //
    //    @Test
    //    void fail_fast_if_multiple_created_timestamps() {
    //        var thrown = assertThrows(InvalidDepositException.class,
    //            () -> createTaskWrapper("deposit3_2created"));
    //        assertTrue(thrown.getMessage().contains("Value 'created' should contain exactly 1 value in bag; 2 found"));
    //    }
}
