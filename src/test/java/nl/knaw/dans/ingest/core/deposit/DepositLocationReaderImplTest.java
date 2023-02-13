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

import org.apache.commons.configuration2.BaseConfiguration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DepositLocationReaderImplTest {

    @Test
    void getTarget_should_return_sword_token() throws Throwable {
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        Mockito.doReturn(Path.of("bagdir"))
            .when(bagDirResolver).getValidBagDir(Mockito.any());

        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", "token");
        config.setProperty("identifier.doi", "doi");

        var reader = new DepositLocationReaderImpl(bagDirResolver);
        var result = reader.getTarget(config);

        assertEquals("token", result);
    }

    @Test
    void getTarget_should_return_doi_if_sword_token_is_null() throws Throwable {
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        Mockito.doReturn(Path.of("bagdir"))
            .when(bagDirResolver).getValidBagDir(Mockito.any());

        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", null);
        config.setProperty("identifier.doi", "doi");

        var reader = new DepositLocationReaderImpl(bagDirResolver);
        var result = reader.getTarget(config);

        assertEquals("doi", result);
    }

    @Test
    void getTarget_should_return_doi_if_sword_token_is_blank() throws Throwable {
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        Mockito.doReturn(Path.of("bagdir"))
            .when(bagDirResolver).getValidBagDir(Mockito.any());

        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", " ");
        config.setProperty("identifier.doi", "doi");

        var reader = new DepositLocationReaderImpl(bagDirResolver);
        var result = reader.getTarget(config);

        assertEquals("doi", result);
    }

    @Test
    void getTarget_should_return_empty_string_if_no_targets_are_set() throws Throwable {
        var bagDirResolver = Mockito.mock(BagDirResolver.class);
        Mockito.doReturn(Path.of("bagdir"))
            .when(bagDirResolver).getValidBagDir(Mockito.any());

        var config = new BaseConfiguration();
        config.setProperty("dataverse.sword-token", " ");
        config.setProperty("identifier.doi", " ");

        var reader = new DepositLocationReaderImpl(bagDirResolver);
        var result = reader.getTarget(config);

        assertEquals("", result);
    }
}