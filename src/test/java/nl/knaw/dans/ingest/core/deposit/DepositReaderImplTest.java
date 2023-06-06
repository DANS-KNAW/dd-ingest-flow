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
package nl.knaw.dans.ingest.core.deposit;

import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.Metadata;
import gov.loc.repository.bagit.reader.BagReader;
import nl.knaw.dans.ingest.core.domain.DepositFile;
import nl.knaw.dans.ingest.core.exception.InvalidDepositException;
import nl.knaw.dans.ingest.core.io.BagDataManager;
import nl.knaw.dans.ingest.core.io.BagDataManagerImpl;
import nl.knaw.dans.ingest.core.io.FileService;
import nl.knaw.dans.ingest.core.io.FileServiceImpl;
import nl.knaw.dans.ingest.core.service.XmlReader;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DepositReaderImplTest {
    private final Path testDir = new File("target/test/" + getClass().getSimpleName()).toPath();

    @BeforeEach
    void clear() {
        FileUtils.deleteQuietly(testDir.toFile());
    }

    private DepositFileLister getDepositFileLister() {
        return deposit -> List.of(
            new DepositFile(Path.of("data/file1.txt"), Path.of("file1.txt"), "text/plain", null),
            new DepositFile(Path.of("data/file2.txt"), Path.of("file2.txt"), "text/plain", null)
        );
    }

    @Test
    void readDeposit_should_call_with_correct_paths() throws Throwable {
        var basePath = Path.of("/some/path/to/4e97185d-b38c-4ed9-bdf6-64339acfb6e8");
        var xmlReader = Mockito.mock(XmlReader.class);
        var fileService = Mockito.mock(FileService.class);
        var depositFileLister = getDepositFileLister();

        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        Mockito.doReturn(basePath.resolve("bagdir"))
            .when(bagDirResolver).getBagDir(Mockito.any());

        var bagDataManager = Mockito.mock(BagDataManager.class);
        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", "token");

        var metadata = new Metadata();
        metadata.add("Created", "2022-10-01T00:03:04+03:00");
        var bag = new Bag();
        bag.setMetadata(metadata);

        Mockito.doReturn(bag)
            .when(bagDataManager).readBag(Mockito.eq(basePath.resolve("bagdir")));

        Mockito.doReturn(config)
            .when(bagDataManager).readDepositProperties(Mockito.any());

        var reader = new DepositReaderImpl(xmlReader, bagDirResolver, fileService, bagDataManager, depositFileLister);
        var result = reader.readDeposit(basePath);

        // check if the bagDataManager is called with the correct paths
        Mockito.verify(bagDataManager).readBag(Mockito.eq(basePath.resolve("bagdir")));
        Mockito.verify(bagDataManager).readDepositProperties(Mockito.eq(basePath));

        assertEquals("4e97185d-b38c-4ed9-bdf6-64339acfb6e8", result.getDepositId());
    }

    @Test
    void mapToDeposit_should_return_new_deposit_for_sparse_data() {
        var xmlReader = Mockito.mock(XmlReader.class);
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        var fileService = Mockito.mock(FileService.class);
        var bagDataManager = Mockito.mock(BagDataManager.class);
        var depositFileLister = getDepositFileLister();

        var reader = new DepositReaderImpl(xmlReader, bagDirResolver, fileService, bagDataManager, depositFileLister);
        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", "token");
        config.setProperty("identifier.doi", "doi");
        config.setProperty("depositor.userId", "user001");

        var bag = new Bag();
        var deposit = reader.mapToDeposit(Path.of("somepath"), Path.of("another"), config, bag);

        assertEquals(Path.of("somepath"), deposit.getDir());
        assertEquals(Path.of("another"), deposit.getBagDir());

        assertEquals("user001", deposit.getDepositorUserId());
        assertFalse(deposit.isUpdate());
    }

    @Test
    void mapToDeposit_should_return_update_deposit_for_is_version_of() {
        var xmlReader = Mockito.mock(XmlReader.class);
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        var fileService = Mockito.mock(FileService.class);
        var bagDataManager = Mockito.mock(BagDataManager.class);
        var depositFileLister = getDepositFileLister();

        var reader = new DepositReaderImpl(xmlReader, bagDirResolver, fileService, bagDataManager, depositFileLister);
        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", "token");
        config.setProperty("identifier.doi", "doi");
        config.setProperty("depositor.userId", "user001");

        var bag = new Bag();
        bag.getMetadata().add("is-version-of", "version1.0");
        var deposit = reader.mapToDeposit(Path.of("somepath"), Path.of("another"), config, bag);

        assertEquals(Path.of("somepath"), deposit.getDir());
        assertEquals(Path.of("another"), deposit.getBagDir());

        assertEquals("user001", deposit.getDepositorUserId());
        assertTrue(deposit.isUpdate());
    }
}