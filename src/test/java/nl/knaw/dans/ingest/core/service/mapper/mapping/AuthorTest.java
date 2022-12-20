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
package nl.knaw.dans.ingest.core.service.mapper.mapping;

import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class AuthorTest extends BaseTest {

    @Test
    void toAuthorValueObject_should_map_creator() throws Exception {

        var doc = readDocumentFromString("<dc:creator xmlns:dc=\"http://purl.org/dc/elements/1.1/\">Author Name</dc:creator>");
        var builder = new CompoundFieldBuilder("", true);
        Author.toAuthorValueObject.build(builder, doc.getDocumentElement());
        assertThat(builder.build().getValue())
            .extracting(AUTHOR_NAME)
            .extracting("value")
            .containsOnly("Author Name");
    }

    @Test
    void toAuthorValueObject_should_map_creatorDetails_for_author_node() throws Exception {

        var doc = readDocumentFromString(
            "<dcx-dai:creatorDetails\n"
                + "    xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
                + "    <dcx-dai:author>\n"
                + "        <dcx-dai:titles>T</dcx-dai:titles>\n"
                + "        <dcx-dai:initials>I</dcx-dai:initials>\n"
                + "        <dcx-dai:insertions>D</dcx-dai:insertions>\n"
                + "        <dcx-dai:surname>Lastname</dcx-dai:surname>\n"
                + "        <dcx-dai:organization>\n"
                + "            <dcx-dai:name xml:lang=\"en\">Example Org</dcx-dai:name>\n"
                + "        </dcx-dai:organization>\n"
                + "    </dcx-dai:author>\n"
                + "</dcx-dai:creatorDetails>");

        var builder = new CompoundFieldBuilder("", true);
        Author.toAuthorValueObject.build(builder, doc.getDocumentElement());
        assertThat(builder.build().getValue())
            .extracting(AUTHOR_NAME)
            .extracting("value")
            .containsOnly("I D Lastname");
    }

    @Test
    void toAuthorValueObject_should_map_creatorDetails_for_organization_node() throws Exception {

        var doc = readDocumentFromString(
            "<dcx-dai:creatorDetails\n"
                + "    xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
                + "    <dcx-dai:organization>\n"
                + "        <dcx-dai:name xml:lang=\"en\">Anti-Vampire League</dcx-dai:name>\n"
                + "        <dcx-dai:role xml:lang=\"en\">DataCurator</dcx-dai:role>\n"
                + "    </dcx-dai:organization>\n"
                + "</dcx-dai:creatorDetails>");

        var builder = new CompoundFieldBuilder("", true);
        Author.toAuthorValueObject.build(builder, doc.getDocumentElement());
        assertThat(builder.build().getValue())
            .extracting(AUTHOR_NAME)
            .extracting("value")
            .containsOnly("Anti-Vampire League");
    }

}