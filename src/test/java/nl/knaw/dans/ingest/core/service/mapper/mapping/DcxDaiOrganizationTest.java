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

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_IDENTIFIER_SCHEME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DcxDaiOrganizationTest extends BaseTest {

    @Test
    void toContributorValueObject_should_create_correct_contributor_details_in_Json_object() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:organization xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "    <dcx-dai:name xml:lang=\"en\">Anti-Vampire League</dcx-dai:name>\n"
            + "    <dcx-dai:role xml:lang=\"en\">DataCurator</dcx-dai:role>\n"
            + "</dcx-dai:organization>\n");

        var builder = new CompoundFieldBuilder("", false);
        DcxDaiOrganization.toContributorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue()).extracting(CONTRIBUTOR_NAME).extracting("value")
            .containsOnly("Anti-Vampire League");

        assertThat(field.getValue()).extracting(CONTRIBUTOR_TYPE).extracting("value")
            .containsOnly("Data Curator");
    }

    @Test
    void toContributorValueObject_should_give_other_as_contributor_type() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:organization xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "    <dcx-dai:role xml:lang=\"en\">ContactPerson</dcx-dai:role>\n"
            + "</dcx-dai:organization>\n");

        var builder = new CompoundFieldBuilder("", false);
        DcxDaiOrganization.toContributorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue()).extracting(CONTRIBUTOR_TYPE).extracting("value")
            .containsOnly("Other");
    }

    @Test
    void toAuthorValueObject_should_use_organization_name_as_author_name() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:organization xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "    <dcx-dai:name xml:lang=\"en\">Anti-Vampire League</dcx-dai:name>\n"
            + "</dcx-dai:organization>\n");

        var builder = new CompoundFieldBuilder("", false);
        DcxDaiOrganization.toAuthorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue()).extracting(AUTHOR_NAME).extracting("value")
            .containsOnly("Anti-Vampire League");
    }

    @Test
    void toAuthorValueObject_should_use_ISNI_if_present() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:organization xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "    <dcx-dai:name xml:lang=\"en\">Anti-Vampire League</dcx-dai:name>\n"
            + "    <dcx-dai:ISNI>http://isni.org/isni/0000000121032683</dcx-dai:ISNI>"
            + "</dcx-dai:organization>\n");

        var builder = new CompoundFieldBuilder("", false);
        DcxDaiOrganization.toAuthorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue()).extracting(AUTHOR_NAME).extracting("value")
            .containsOnly("Anti-Vampire League");
        assertThat(field.getValue()).extracting(AUTHOR_IDENTIFIER_SCHEME).extracting("value")
            .containsOnly("ISNI");
        assertThat(field.getValue()).extracting(AUTHOR_IDENTIFIER).extracting("value")
            .containsOnly("0000000121032683");
    }

    @Test
    void toAuthorValueObject_should_use_VIAF_if_present() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:organization xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "    <dcx-dai:name xml:lang=\"en\">Anti-Vampire League</dcx-dai:name>\n"
            + "    <dcx-dai:VIAF>http://viaf.org/viaf/141399695</dcx-dai:VIAF>"
            + "</dcx-dai:organization>\n");

        var builder = new CompoundFieldBuilder("", false);
        DcxDaiOrganization.toAuthorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue()).extracting(AUTHOR_NAME).extracting("value")
            .containsOnly("Anti-Vampire League");
        assertThat(field.getValue()).extracting(AUTHOR_IDENTIFIER_SCHEME).extracting("value")
            .containsOnly("VIAF");
        assertThat(field.getValue()).extracting(AUTHOR_IDENTIFIER).extracting("value")
            .containsOnly("141399695");
    }

    @Test
    void toGrantNumberValueObject_should_create_a_grantnumber_with_only_an_organization_subfield() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:organization xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "    <dcx-dai:name xml:lang=\"en\">Anti-Vampire League</dcx-dai:name>\n"
            + "    <dcx-dai:role xml:lang=\"en\">Funder</dcx-dai:role>\n"
            + "    <dcx-dai:ISNI>http://isni.org/isni/0000000121032683</dcx-dai:ISNI>"
            + "</dcx-dai:organization>\n");

        assertTrue(DcxDaiOrganization.isFunder(doc.getDocumentElement()));

        var builder = new CompoundFieldBuilder("", false);
        DcxDaiOrganization.toGrantNumberValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue()).extracting(GRANT_NUMBER_VALUE).extracting("value")
            .containsOnly("");
        assertThat(field.getValue()).extracting(GRANT_NUMBER_AGENCY).extracting("value")
            .containsOnly("Anti-Vampire League");
    }
}