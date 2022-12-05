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

import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

class DescriptionTest extends BaseTest {

    @Test
    void getDescription_should_return_a_html_formatted_response() throws Exception {
        var doc = readDocumentFromString("<dc:description xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n"
            + "    Lorem ipsum dolor sit amet,\n"
            + "\n"
            + "    consectetur adipiscing elit.\n"
            + "    Lorem ipsum.\n"
            + "</dc:description>");

        var builder = new CompoundFieldBuilder("", false);
        Description.toDescription.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue())
            .extracting(DESCRIPTION_VALUE)
            .extracting("value")
            .containsOnly("<p>Lorem ipsum dolor sit amet,</p><p>consectetur adipiscing elit.<br>Lorem ipsum.</p>");
    }
}