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

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class SpatialCoverageTest extends BaseTest {

    @Test
    void has_child_element() throws Exception {
        var doc = readDocumentFromString("<node><child>text</child> text</node>");
        var root = doc.getDocumentElement();
        assertTrue(SpatialCoverage.hasChildElement(root));
    }

    @Test
    void has_no_child_element() throws Exception {
        var doc = readDocumentFromString("<node><child>text</child> text</node>");
        var root = doc.getDocumentElement().getFirstChild();
        assertFalse(SpatialCoverage.hasChildElement(root));
    }

    // TODO this should not be here
    @Test
    void test_field_builder() throws Exception {
        var builder = new CompoundFieldBuilder("TEST", true);
        builder.addSubfield("field1", "value1")
            .addSubfield("field2", "value2")
            .addControlledSubfield("field3", "x");

        builder.nextValue();
        builder.addSubfield("field1", "value3")
            .addSubfield("field2", "value4")
            .addControlledSubfield("field3", "x");

        builder.nextValue();
        var result = builder.build();
        var str = new ObjectMapper()
            .writer()
            .withDefaultPrettyPrinter()
            .writeValueAsString(result);
        log.debug("result: {}", str);
    }
}