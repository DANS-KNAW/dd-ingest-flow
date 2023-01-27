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

import gov.loc.repository.bagit.reader.BagReader;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class ManifestHelperTest {
    Path testDir = new File("target/test/" + getClass().getSimpleName()).toPath();

    @BeforeEach
    void clear() {
        testDir.toFile().delete();
    }

    @Test
    void getFilePathToSha1() throws Exception {
        var bagDir = new File("src/test/resources/examples/valid-easy-submitted-no-doi/example-bag-medium").toPath();
        var deposit = new Deposit();
        deposit.setBagDir(bagDir);
        deposit.setBag(new BagReader().read(bagDir));

        var result = ManifestHelper.getFilePathToSha1(deposit.getBag());
        assertThat(result).hasSize(5);
    }

    @Test
    void addSha1ToBag() throws Exception {
        var originalBag = "src/test/resources/examples/valid-easy-submitted-no-doi/example-bag-medium";
        var bagDir = testDir.resolve("bag");
        var sha1File = bagDir.resolve("manifest-sha1.txt").toFile();
        FileUtils.copyDirectory(new File(originalBag), bagDir.toFile());
        FileUtils.deleteQuietly(sha1File);

        ManifestHelper.ensureSha1ManifestPresent(new BagReader().read(bagDir));
        assertThat(sha1File).exists();
    }
}
