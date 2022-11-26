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
package nl.knaw.dans.ingest.core.service.mapping;

import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class FileElementTest extends BaseTest {

    @Test
    void to_file_meta() throws Exception {
        var doc = readDocument("files.xml");
        var node = XPathEvaluator.nodes(doc, "//files:files/files:file[1]").findFirst().orElseThrow();

        var result = FileElement.toFileMeta(node, true);

        assertEquals("leeg.txt", result.getLabel());
        assertNull( result.getDirectoryLabel());
        assertEquals("description: \"Empty file\"; time_period: \"Classical\"; hardware: \"Hardware\"", result.getDescription());
        assertEquals(true, result.getRestricted());
    }
    @Test
    void to_file_meta_with_subdir() throws Exception {
        var doc = readDocument("files.xml");
        var node = XPathEvaluator.nodes(doc, "//files:files/files:file[4]").findFirst().orElseThrow();

        var result = FileElement.toFileMeta(node, true);

        assertEquals("sine-md5.txt", result.getLabel());
        assertEquals("sub/sub", result.getDirectoryLabel());
        assertEquals("description: \"Another empty file\"; hardware: \"Hardware\"", result.getDescription());
        assertEquals(true, result.getRestricted());
    }

}