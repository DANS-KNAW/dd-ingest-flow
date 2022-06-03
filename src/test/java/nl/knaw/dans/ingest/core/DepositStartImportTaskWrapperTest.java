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

import better.files.File;
import nl.knaw.dans.easy.dd2d.Deposit;
import nl.knaw.dans.easy.dd2d.DepositMigrationTask;
import nl.knaw.dans.easy.dd2d.ZipFileHandler;
import nl.knaw.dans.ingest.core.legacy.DepositImportTaskWrapper;
import org.junit.jupiter.api.Test;
import scala.Option;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeParseException;
import java.util.List;
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

    @Test
    public void depositsShouldBeOrderedByCreatedTimestamp() {
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
    public void failFastIfNoTimeZoneInCreatedTimestamp() {
        assertThrows(DateTimeParseException.class, () -> createTaskWrapper("deposit3_notimezone"));
    }

    @Test
    public void failFastIfNoCreatedTimestamp() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> createTaskWrapper("deposit3_nocreated"));
        assertTrue(thrown.getMessage().contains("No Created value found in bag"));
    }

    @Test
    public void failFastIfMultipleCreatedTimestamps() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> createTaskWrapper("deposit3_2created"));
        assertTrue(thrown.getMessage().contains("There should be exactly one Created value; found 2"));
    }

    private static DepositImportTaskWrapper createTaskWrapper(String depositName) {
        return new DepositImportTaskWrapper(new DepositMigrationTask(
            new Deposit(File.apply(testDepositsBasedir.resolve(depositName))),
            Option.empty(),
            new ZipFileHandler(File.apply(Paths.get("dummy"))),
            "dummy",
            false,
            null,
            Option.empty(),
            null,
            null,
            Option.empty(),
            0,
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ), null);
    }
}
