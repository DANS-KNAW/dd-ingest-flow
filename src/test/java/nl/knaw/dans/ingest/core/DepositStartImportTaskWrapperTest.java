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

import nl.knaw.dans.ingest.core.legacy.DepositImportTaskWrapper;
import nl.knaw.dans.ingest.core.service.DansBagValidator;
import nl.knaw.dans.ingest.core.service.DepositManagerImpl;
import nl.knaw.dans.ingest.core.service.DepositMigrationTask;
import nl.knaw.dans.ingest.core.service.DepositToDvDatasetMetadataMapper;
import nl.knaw.dans.ingest.core.service.EventWriter;
import nl.knaw.dans.ingest.core.service.XmlReader;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import nl.knaw.dans.ingest.core.service.ZipFileHandler;
import nl.knaw.dans.ingest.core.service.exception.InvalidDepositException;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    private final Set<String> activeMetadataBlocks = Set.of("citation", "dansRights", "dansRelationalMetadata", "dansArchaeologyMetadata", "dansTemporalSpation", "dansDataVaultMetadata");
    private final XmlReader xmlReader = new XmlReaderImpl();

    private final Map<String, String> iso1ToDataverseLanguage = new HashMap<>();
    private final Map<String, String> iso2ToDataverseLanguage = new HashMap<>();

    private DepositImportTaskWrapper createTaskWrapper(String depositName) throws Throwable {

        var client = Mockito.mock(DataverseClient.class);
        var mapper = getMapper();
        var validator = Mockito.mock(DansBagValidator.class);
        var eventWriter = Mockito.mock(EventWriter.class);
        var depositManager = new DepositManagerImpl(new XmlReaderImpl());
        var deposit = depositManager.loadDeposit(testDepositsBasedir.resolve(depositName));

        var task = new DepositMigrationTask(
            mapper, deposit, client, "dummy",
            null,
            new ZipFileHandler(Path.of("target/test")),
            Map.of(),
            List.of(),
            validator,
            1000,
            1000,
            Path.of("dummy"),
            eventWriter,
            depositManager
        );
        return new DepositImportTaskWrapper(task, eventWriter);
    }

    @BeforeEach
    void setUp() {
        iso1ToDataverseLanguage.put("nl", "Dutch");
        iso1ToDataverseLanguage.put("de", "German");

        iso2ToDataverseLanguage.put("dut", "Dutch");
        iso2ToDataverseLanguage.put("ger", "German");
    }

    DepositToDvDatasetMetadataMapper getMapper() {
        return new DepositToDvDatasetMetadataMapper(
            true, activeMetadataBlocks, iso1ToDataverseLanguage, iso2ToDataverseLanguage
        );
    }

    @Test
    void deposits_should_be_ordered_by_created_timestamp() throws Throwable {
        List<DepositImportTaskWrapper> sorted = Stream.of(
            createTaskWrapper("deposit2_a"),
            createTaskWrapper("deposit1_b"),
            createTaskWrapper("deposit1_a"),
            createTaskWrapper("deposit1_first"),
            createTaskWrapper("deposit2_first")
        ).sorted().collect(Collectors.toList());

        assertEquals("10.5072/deposit2_first", sorted.get(0).getTarget());
        assertEquals("10.5072/deposit1_first", sorted.get(1).getTarget());
        assertEquals("10.5072/deposit1_a", sorted.get(2).getTarget());
        assertEquals("10.5072/deposit1_b", sorted.get(3).getTarget());
        assertEquals("10.5072/deposit2_a", sorted.get(4).getTarget());
    }

    @Test
    void fail_fast_if_no_time_zone_in_created_timestamp() {
        var thrown = assertThrows(InvalidDepositException.class,
            () -> createTaskWrapper("deposit3_notimezone"));

        assertEquals(thrown.getCause().getClass(), DateTimeParseException.class);
    }

    @Test
    void fail_fast_if_no_created_timestamp() {
        var thrown = assertThrows(InvalidDepositException.class,
            () -> createTaskWrapper("deposit3_nocreated"));
        assertTrue(thrown.getMessage().contains("No 'Created' value found in bag"));
    }

    @Test
    void fail_fast_if_multiple_created_timestamps() {
        var thrown = assertThrows(InvalidDepositException.class,
            () -> createTaskWrapper("deposit3_2created"));
        assertTrue(thrown.getMessage().contains("Value 'Created' should contain exactly 1 value in bag; 2 found"));
    }
}
