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
import gov.loc.repository.bagit.reader.BagReader;
import nl.knaw.dans.ingest.core.io.BagDataManager;
import nl.knaw.dans.ingest.core.io.FileService;
import nl.knaw.dans.ingest.core.service.XmlReader;
import org.apache.commons.configuration2.BaseConfiguration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DepositReaderImplTest {
    @Test
    void mapToDeposit_should_return_new_deposit_for_sparse_data() {
        var xmlReader = Mockito.mock(XmlReader.class);
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        var fileService = Mockito.mock(FileService.class);
        var bagDataManager = Mockito.mock(BagDataManager.class);

        var reader = new DepositReaderImpl(xmlReader, bagDirResolver, fileService, bagDataManager);
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

        var reader = new DepositReaderImpl(xmlReader, bagDirResolver, fileService, bagDataManager);
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