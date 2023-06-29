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

import gov.loc.repository.bagit.reader.BagReader;
import nl.knaw.dans.ingest.core.exception.InvalidDepositException;
import nl.knaw.dans.ingest.core.io.BagDataManagerImpl;
import nl.knaw.dans.ingest.core.io.FileServiceImpl;
import nl.knaw.dans.ingest.core.service.ManifestHelperImpl;
import nl.knaw.dans.ingest.core.service.XmlReaderImpl;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DepositFileListerImplIntegrationTest {

    @Test
    void getDepositFiles_should_list_files_with_original_filepaths() throws Exception, InvalidDepositException {

        var lister = new DepositFileListerImpl();
        var fileService = new FileServiceImpl();
        var depositReader = new DepositReaderImpl(
                new XmlReaderImpl(),
                new BagDirResolverImpl(fileService),
                fileService,
                new BagDataManagerImpl(new BagReader()),
                lister,
                new ManifestHelperImpl()
        );

        var path = Path.of(getClass().getResource("/examples/valid-with-original-filepaths").toURI().getPath());
        var deposit = depositReader.readDeposit(path);

        var files = deposit.getFiles();

        assertThat(files).extracting("path")
                .containsOnly(
                        Path.of("data/random images/image01.png"),
                        Path.of("data/random images/image02.jpeg"),
                        Path.of("data/random images/image03.jpeg"),
                        Path.of("data/a/deeper/path/With some file.txt")
                );

        assertThat(files).extracting("physicalPath")
                .containsOnly(
                        Path.of("data/aa2345ab-bff5-49c9-b224-f8d3df0fd37a"),
                        Path.of("data/57f6f2f8-8d87-43ec-ac0e-68bdac21223e"),
                        Path.of("data/79c713b0-b232-4aaa-80cc-9bc34111acf7"),
                        Path.of("data/26e30e9b-64a8-4a2f-8c70-a4653219c984")
                );
    }
}