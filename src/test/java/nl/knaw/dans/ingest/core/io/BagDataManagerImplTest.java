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
package nl.knaw.dans.ingest.core.io;

import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.reader.BagReader;
import gov.loc.repository.bagit.verify.BagVerifier;
import nl.knaw.dans.ingest.core.domain.Deposit;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BagDataManagerImplTest {
    private final Path testDir = new File("target/test/" + getClass().getSimpleName()).toPath();
    private final BagReader bagReader = new BagReader();

    private final BagVerifier bagVerifier = new BagVerifier();

    @BeforeEach
    void clear() {
        testDir.toFile().delete();
    }

    @Test
    public void writeBagMetadata_should_keep_bag_valid() throws Exception {
        var bagDir = testDir.resolve("bag");
        FileUtils.copyDirectory(Paths.get("src/test/resources/examples/valid-easy-submitted-no-doi/example-bag-medium").toFile(), bagDir.toFile());
        var startBag = bagReader.read(bagDir);
        bagVerifier.isValid(startBag, false); // Check that bag is valid to begin with
        startBag.getMetadata().add("Test-Entry", "test");
        var bagDataManagerImpl = new BagDataManagerImpl(null);
        bagDataManagerImpl.writeBagMetadata(startBag);
        final Bag endBag = bagReader.read(bagDir);
        Assertions.assertDoesNotThrow(() -> bagVerifier.isValid(endBag, false), "Is valid should not throw an exception");
    }

}
