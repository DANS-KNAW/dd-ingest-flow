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

import gov.loc.repository.bagit.domain.Metadata;
import nl.knaw.dans.ingest.core.exception.MissingTargetException;
import nl.knaw.dans.ingest.core.io.BagDataManager;
import org.apache.commons.configuration2.BaseConfiguration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DepositLocationReaderImplTest {

    @Test
    void readDepositLocation_should_call_with_correct_paths() throws Throwable {
        var basePath = Path.of("/some/path/to/4e97185d-b38c-4ed9-bdf6-64339acfb6e8");
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        Mockito.doReturn(basePath.resolve("bagdir"))
            .when(bagDirResolver).getValidBagDir(Mockito.any());

        var bagDataManager = Mockito.mock(BagDataManager.class);
        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", "token");
        config.setProperty("identifier.doi", "doi");

        Mockito.doReturn(config)
            .when(bagDataManager).readDepositProperties(Mockito.any());

        var metadata = new Metadata();
        metadata.add("Created", "2022-10-01T00:03:04+03:00");

        Mockito.doReturn(metadata).when(bagDataManager).readBagMetadata(Mockito.any());

        var reader = new DepositLocationReaderImpl(bagDirResolver, bagDataManager);
        var result = reader.readDepositLocation(basePath);

        Mockito.verify(bagDataManager).readBagMetadata(Mockito.eq(basePath.resolve("bagdir")));
        Mockito.verify(bagDataManager).readDepositProperties(Mockito.eq(basePath));

        assertEquals("4e97185d-b38c-4ed9-bdf6-64339acfb6e8", result.getDepositId());
    }

    @Test
    void getTarget_should_return_sword_token() throws Throwable {
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        Mockito.doReturn(Path.of("bagdir"))
            .when(bagDirResolver).getValidBagDir(Mockito.any());

        var bagDataManager = Mockito.mock(BagDataManager.class);
        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", "token");
        config.setProperty("identifier.doi", "doi");

        var reader = new DepositLocationReaderImpl(bagDirResolver, bagDataManager);
        var result = reader.getTarget(config);

        assertEquals("token", result);
    }

    @Test
    void getTarget_should_return_doi_if_sword_token_is_null() throws Throwable {
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        Mockito.doReturn(Path.of("bagdir"))
            .when(bagDirResolver).getValidBagDir(Mockito.any());

        var bagDataManager = Mockito.mock(BagDataManager.class);
        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", null);
        config.setProperty("identifier.doi", "doi");

        var reader = new DepositLocationReaderImpl(bagDirResolver, bagDataManager);
        var result = reader.getTarget(config);

        assertEquals("doi", result);
    }

    @Test
    void getTarget_should_return_doi_if_sword_token_is_blank() throws Throwable {
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        Mockito.doReturn(Path.of("bagdir"))
            .when(bagDirResolver).getValidBagDir(Mockito.any());

        var bagDataManager = Mockito.mock(BagDataManager.class);
        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", " ");
        config.setProperty("identifier.doi", "doi");

        var reader = new DepositLocationReaderImpl(bagDirResolver, bagDataManager);
        var result = reader.getTarget(config);

        assertEquals("doi", result);
    }

    @Test
    void getTarget_should_throw_MissingTargetException_when_targets_are_missing() throws Throwable {
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        Mockito.doReturn(Path.of("bagdir"))
            .when(bagDirResolver).getValidBagDir(Mockito.any());

        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", " ");
        config.setProperty("identifier.doi", " ");

        var bagDataManager = Mockito.mock(BagDataManager.class);

        var reader = new DepositLocationReaderImpl(bagDirResolver, bagDataManager);

        assertThrows(MissingTargetException.class, () -> reader.getTarget(config));
    }
}