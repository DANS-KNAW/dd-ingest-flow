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
import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_AFFILIATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DcxDaiAuthorTest extends BaseTest {

    @Test
    void toAuthorValueObject_should_create_correct_author_details_in_Json_object() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:author xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "                <dcx-dai:titles>Prof.</dcx-dai:titles>\n"
            + "                <dcx-dai:initials>D.N.</dcx-dai:initials>\n"
            + "                <dcx-dai:insertions>van den</dcx-dai:insertions>\n"
            + "                <dcx-dai:surname>Aarden</dcx-dai:surname>\n"
            + "                <dcx-dai:DAI>nfo:eu-repo/dai/nl/07193567X</dcx-dai:DAI>\n"
            + "                <dcx-dai:ORCID>https://orcid.org/0000-0001-6438-5123</dcx-dai:ORCID>\n"
            + "                <dcx-dai:ISNI>http://isni.org/isni/0000000396540123</dcx-dai:ISNI>\n"
            + "                <dcx-dai:role>Distributor</dcx-dai:role>\n"
            + "                <dcx-dai:organization>\n"
            + "                    <dcx-dai:name xml:lang=\"en\">Utrecht University</dcx-dai:name>\n"
            + "                </dcx-dai:organization>\n"
            + "            </dcx-dai:author>");

        var builder = new CompoundFieldBuilder("", true);
        DcxDaiAuthor.toAuthorValueObject.build(builder, doc.getDocumentElement());
        var field = (CompoundMultiValueField) builder.build();

        assertThat(field.getValue()).extracting(AUTHOR_NAME).extracting("value")
            .containsOnly("D.N. van den Aarden");

        assertThat(field.getValue()).extracting(AUTHOR_AFFILIATION).extracting("value")
            .containsOnly("Utrecht University");

        assertThat(field.getValue()).extracting(AUTHOR_IDENTIFIER_SCHEME).extracting("value")
            .containsOnly("ORCID");

        assertThat(field.getValue()).extracting(AUTHOR_IDENTIFIER).extracting("value")
            .containsOnly("0000-0001-6438-5123");
    }

    @Test
    void toAuthorValueObject_should_create_correct_ISNI_in_Json_object() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:author xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "                <dcx-dai:titles>Prof.</dcx-dai:titles>\n"
            + "                <dcx-dai:initials>D.N.</dcx-dai:initials>\n"
            + "                <dcx-dai:insertions>van den</dcx-dai:insertions>\n"
            + "                <dcx-dai:surname>Aarden</dcx-dai:surname>\n"
            + "                <dcx-dai:DAI>nfo:eu-repo/dai/nl/07193567X</dcx-dai:DAI>\n"
            + "                <dcx-dai:ISNI>http://isni.org/isni/0000000396540123</dcx-dai:ISNI>\n"
            + "                <dcx-dai:role>Distributor</dcx-dai:role>\n"
            + "                <dcx-dai:organization>\n"
            + "                    <dcx-dai:name xml:lang=\"en\">Utrecht University</dcx-dai:name>\n"
            + "                </dcx-dai:organization>\n"
            + "            </dcx-dai:author>");

        var builder = new CompoundFieldBuilder("", true);
        DcxDaiAuthor.toAuthorValueObject.build(builder, doc.getDocumentElement());
        var field = (CompoundMultiValueField) builder.build();

        assertThat(field.getValue()).extracting(AUTHOR_NAME).extracting("value")
            .containsOnly("D.N. van den Aarden");

        assertThat(field.getValue()).extracting(AUTHOR_AFFILIATION).extracting("value")
            .containsOnly("Utrecht University");

        assertThat(field.getValue()).extracting(AUTHOR_IDENTIFIER_SCHEME).extracting("value")
            .containsOnly("ISNI");

        assertThat(field.getValue()).extracting(AUTHOR_IDENTIFIER).extracting("value")
            .containsOnly("0000000396540123");
    }
    @Test
    void toContributorValueObject_should_create_correct_contributor_details_in_Json_object() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:author xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "                <dcx-dai:titles>Prof.</dcx-dai:titles>\n"
            + "                <dcx-dai:initials>D.N.</dcx-dai:initials>\n"
            + "                <dcx-dai:insertions>van den</dcx-dai:insertions>\n"
            + "                <dcx-dai:surname>Aarden</dcx-dai:surname>\n"
            + "                <dcx-dai:DAI>nfo:eu-repo/dai/nl/07193567X</dcx-dai:DAI>\n"
            + "                <dcx-dai:ORCID>0000-0001-6438-5123</dcx-dai:ORCID>\n"
            + "                <dcx-dai:ISNI>0000000396540123</dcx-dai:ISNI>\n"
            + "                <dcx-dai:role>ProjectManager</dcx-dai:role>\n"
            + "                <dcx-dai:organization>\n"
            + "                    <dcx-dai:name xml:lang=\"en\">Utrecht University</dcx-dai:name>\n"
            + "                </dcx-dai:organization>\n"
            + "            </dcx-dai:author>");

        var builder = new CompoundFieldBuilder("", true);
        DcxDaiAuthor.toContributorValueObject.build(builder, doc.getDocumentElement());
        var field = (CompoundMultiValueField) builder.build();

        assertThat(field.getValue()).extracting(CONTRIBUTOR_NAME).extracting("value")
            .containsOnly("D.N. van den Aarden (Utrecht University)");

        assertThat(field.getValue()).extracting(CONTRIBUTOR_TYPE).extracting("value")
            .containsOnly("Project Manager");
    }

    @Test
    void toContributorValueObject_should_give_organization_name_as_contributor_name_and_other_as_contributor_type() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:author xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "                <dcx-dai:role>Contributor</dcx-dai:role>\n"
            + "                <dcx-dai:organization>\n"
            + "                    <dcx-dai:name xml:lang=\"en\">Utrecht University</dcx-dai:name>\n"
            + "                </dcx-dai:organization>\n"
            + "            </dcx-dai:author>");

        var builder = new CompoundFieldBuilder("", true);
        DcxDaiAuthor.toContributorValueObject.build(builder, doc.getDocumentElement());
        var field = (CompoundMultiValueField) builder.build();

        assertThat(field.getValue()).extracting(CONTRIBUTOR_NAME).extracting("value")
            .containsOnly("Utrecht University");

        assertThat(field.getValue()).extracting(CONTRIBUTOR_TYPE).extracting("value")
            .containsOnly("Other");
    }
    @Test
    void toRightsHolder_should_create_rights_holder_with_organization_in_brackets() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:author xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "                <dcx-dai:titles>Prof.</dcx-dai:titles>\n"
            + "                <dcx-dai:initials>D.N.</dcx-dai:initials>\n"
            + "                <dcx-dai:insertions>van den</dcx-dai:insertions>\n"
            + "                <dcx-dai:surname>Aarden</dcx-dai:surname>\n"
            + "                <dcx-dai:DAI>nfo:eu-repo/dai/nl/07193567X</dcx-dai:DAI>\n"
            + "                <dcx-dai:ORCID>0000-0001-6438-5123</dcx-dai:ORCID>\n"
            + "                <dcx-dai:ISNI>0000000396540123</dcx-dai:ISNI>\n"
            + "                <dcx-dai:role>ProjectManager</dcx-dai:role>\n"
            + "                <dcx-dai:organization>\n"
            + "                    <dcx-dai:name xml:lang=\"en\">Utrecht University</dcx-dai:name>\n"
            + "                </dcx-dai:organization>\n"
            + "            </dcx-dai:author>");

        assertEquals("Prof. D.N. van den Aarden (Utrecht University)",
            DcxDaiAuthor.toRightsHolder(doc.getDocumentElement()));
    }
    @Test
    void toRightsHolder_should_create_rights_holder_with_organization_without_brackets_when_no_surname_is_given() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:author xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "                <dcx-dai:organization>\n"
            + "                    <dcx-dai:name xml:lang=\"en\">Utrecht University</dcx-dai:name>\n"
            + "                </dcx-dai:organization>\n"
            + "            </dcx-dai:author>");

        assertEquals("Utrecht University",
            DcxDaiAuthor.toRightsHolder(doc.getDocumentElement()));
    }
}