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
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.SingleValueField;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

import java.util.List;
import java.util.Map;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_AFFILIATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class AuthorTest extends BaseTest {

    @Test
    void toAuthorValueObject_should_map_creator() throws Exception {

        var doc = readDocumentFromString("<dc:creator xmlns:dc='http://purl.org/dc/elements/1.1/'>Author Name</dc:creator>");
        var value = mapToDV(doc);
        assertThat(value).extracting(AUTHOR_NAME).extracting("value")
            .containsOnly("Author Name");
    }

    @Test
    void toAuthorValueObject_should_map_creatorDetails_for_author_node() throws Exception {

        var doc = readDocumentFromString(""
            + "<dcx-dai:creatorDetails\n"
            + "    xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>\n"
            + "    <dcx-dai:author>\n"
            + "        <dcx-dai:titles>T</dcx-dai:titles>\n"
            + "        <dcx-dai:initials>I</dcx-dai:initials>\n"
            + "        <dcx-dai:insertions>D</dcx-dai:insertions>\n"
            + "        <dcx-dai:surname>Lastname</dcx-dai:surname>\n"
            + "        <dcx-dai:organization>\n"
            + "            <dcx-dai:name xml:lang='en'>Example Org</dcx-dai:name>\n"
            + "        </dcx-dai:organization>\n"
            + "        <dcx-dai:ORCID>https://orcid.org/0000-0002-1825-0097</dcx-dai:ORCID>\n"
            + "        <dcx-dai:ISNI>http://isni.org/isni/000000012281955X</dcx-dai:ISNI>\n"
            + "        <dcx-dai:DAI>info:eu-repo/dai/nl/358163587</dcx-dai:DAI>\n"
            + "    </dcx-dai:author>\n"
            + "</dcx-dai:creatorDetails>");

        var value = mapToDV(doc);
        assertThat(value).extracting(AUTHOR_NAME).extracting("value")
            .containsOnly("I D Lastname");
        assertThat(value).extracting(AUTHOR_AFFILIATION).extracting("value")
            .containsOnly("Example Org");
        assertThat(value).extracting(AUTHOR_IDENTIFIER_SCHEME).extracting("value")
            .containsOnly("ORCID");
        assertThat(value).extracting(AUTHOR_IDENTIFIER).extracting("value")
            .containsOnly("0000-0002-1825-0097");
        assertThat(value.get(0)).hasSize(4); // ISNI and DOI are lost
    }

    @Test
    void toAuthorValueObject_should_map_creatorDetails_for_organisation_node() throws Exception {

        var doc = readDocumentFromString(""
            + "<dcx-dai:creatorDetails\n"
            + "    xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>\n"
            + "    <dcx-dai:organization>\n"
            + "        <dcx-dai:organization>\n"
            + "            <dcx-dai:name xml:lang='en'>Example Org</dcx-dai:name>\n"
            + "        </dcx-dai:organization>\n"
            + "        <dcx-dai:VIAF>viaf</dcx-dai:VIAF>\n"
            + "        <dcx-dai:ISNI>http://isni.org/isni/000000012281955X</dcx-dai:ISNI>\n"
            + "    </dcx-dai:organization>\n"
            + "</dcx-dai:creatorDetails>");

        var value = mapToDV(doc);
        assertThat(value).extracting(AUTHOR_IDENTIFIER_SCHEME).extracting("value")
            .containsOnly("ISNI");
        assertThat(value).extracting(AUTHOR_IDENTIFIER).extracting("value")
            .containsOnly("000000012281955X");
        assertThat(value.get(0)).hasSize(2);
    }

    @Test
    void toAuthorValueObject_should_map_ISNI() throws Exception {

        var doc = readDocumentFromString(""
            + "<dcx-dai:creatorDetails\n"
            + "    xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>\n"
            + "    <dcx-dai:author>\n"
            + "        <dcx-dai:surname>Lastname</dcx-dai:surname>\n"
            + "        <dcx-dai:ISNI>http://isni.org/isni/000000012281955X</dcx-dai:ISNI>\n"
            + "        <dcx-dai:DAI>info:eu-repo/dai/nl/358163587</dcx-dai:DAI>\n"
            + "    </dcx-dai:author>\n"
            + "</dcx-dai:creatorDetails>");

        var value = mapToDV(doc);
        assertThat(value).extracting(AUTHOR_NAME).extracting("value")
            .containsOnly("Lastname");
        assertThat(value).extracting(AUTHOR_IDENTIFIER_SCHEME).extracting("value")
            .containsOnly("ISNI");
        assertThat(value).extracting(AUTHOR_IDENTIFIER).extracting("value")
            .containsOnly("000000012281955X");
        assertThat(value.get(0)).hasSize(3);
    }

    @Test
    void toAuthorValueObject_should_map_DAI() throws Exception {

        var doc = readDocumentFromString(""
            + "<dcx-dai:creatorDetails\n"
            + "    xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>\n"
            + "    <dcx-dai:author>\n"
            + "        <dcx-dai:surname>Lastname</dcx-dai:surname>\n"
            + "        <dcx-dai:DAI>info:eu-repo/dai/nl/358163587</dcx-dai:DAI>\n"
            + "    </dcx-dai:author>\n"
            + "</dcx-dai:creatorDetails>");

        var value = mapToDV(doc);
        assertThat(value).extracting(AUTHOR_NAME).extracting("value")
            .containsOnly("Lastname");
        assertThat(value).extracting(AUTHOR_IDENTIFIER_SCHEME).extracting("value")
            .containsOnly("DAI");
        assertThat(value).extracting(AUTHOR_IDENTIFIER).extracting("value")
            .containsOnly("info:eu-repo/dai/nl/358163587");
        assertThat(value.get(0)).hasSize(3);
    }

    @Test
    void toAuthorValueObject_is_happy_without_name() throws Exception {

        var doc = readDocumentFromString(""
            + "<dcx-dai:creatorDetails\n"
            + "    xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>\n"
            + "    <dcx-dai:author>\n"
            + "        <dcx-dai:DAI>info:eu-repo/dai/nl/358163587</dcx-dai:DAI>\n"
            + "    </dcx-dai:author>\n"
            + "</dcx-dai:creatorDetails>");

        var value = mapToDV(doc);
        assertThat(value).extracting(AUTHOR_IDENTIFIER_SCHEME).extracting("value")
            .containsOnly("DAI");
        assertThat(value).extracting(AUTHOR_IDENTIFIER).extracting("value")
            .containsOnly("info:eu-repo/dai/nl/358163587");
        assertThat(value.get(0)).hasSize(2);
    }

    @Test
    void toAuthorValueObject_should_map_creatorDetails_for_organization_node() throws Exception {

        var doc = readDocumentFromString(""
            + "<dcx-dai:creatorDetails\n"
            + "    xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>\n"
            + "    <dcx-dai:organization>\n"
            + "        <dcx-dai:name>Anti-Vampire League</dcx-dai:name>\n"
            + "        <dcx-dai:role>DataCurator</dcx-dai:role>\n"
            + "    </dcx-dai:organization>\n"
            + "</dcx-dai:creatorDetails>");

        var value = mapToDV(doc);
        assertThat(value).extracting(AUTHOR_NAME).extracting("value")
            .containsOnly("Anti-Vampire League");
        assertThat(value.get(0)).hasSize(1); // no other subfields like role
    }

    private List<Map<String, SingleValueField>> mapToDV(Document doc) {
        var builder = new CompoundFieldBuilder("", true);
        Author.toAuthorValueObject.build(builder, doc.getDocumentElement());
        return ((CompoundMultiValueField) builder.build()).getValue();
    }

}