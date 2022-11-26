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

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER_ID;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.OTHER_ID_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_CITATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_ID_NUMBER;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_ID_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PUBLICATION_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdentifierTest extends BaseTest {

    @Test
    void test_other_id_value_without_agency_for_id_without_type_attribute() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    >\n"
                + "    123\n"
                + "</dct:identifier>");

        var builder = new CompoundFieldBuilder("", false);
        Identifier.toOtherIdValue.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue()).extracting(OTHER_ID_AGENCY).extracting("value")
            .containsOnly("");
        assertThat(field.getValue()).extracting(OTHER_ID_VALUE).extracting("value")
            .containsOnly("123");

    }

    @Test
    void test_other_id_value_with_dans_knaw_for_easy2_attribute() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"id-type:EASY2\">\n"
                + "    easy-dataset:18335\n"
                + "</dct:identifier>");

        var builder = new CompoundFieldBuilder("", false);
        Identifier.toOtherIdValue.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue()).extracting(OTHER_ID_AGENCY).extracting("value")
            .containsOnly("DANS-KNAW");
        assertThat(field.getValue()).extracting(OTHER_ID_VALUE).extracting("value")
            .containsOnly("easy-dataset:18335");

    }

    @Test
    void test_can_be_mapped_to_other_id_return_true_for_easy2_type() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"id-type:EASY2\">\n"
                + "    easy-dataset:18335\n"
                + "</dct:identifier>");

        assertTrue(Identifier.canBeMappedToOtherId(doc.getDocumentElement()));
    }

    @Test
    void test_can_be_mapped_to_other_id_if_no_type_is_present() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    >\n"
                + "    easy-dataset:18335\n"
                + "</dct:identifier>");

        assertTrue(Identifier.canBeMappedToOtherId(doc.getDocumentElement()));
    }

    @Test
    void test_can_be_mapped_to_other_id_if_different_types_are_set() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"id-type:DOI\">\n"
                + "    10.4052/test\n"
                + "</dct:identifier>");

        assertFalse(Identifier.canBeMappedToOtherId(doc.getDocumentElement()));
    }

    @Test
    void test_is_related_publication_if_id_type_is_isbn() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"ISBN\">\n"
                + "    10.4052/test\n"
                + "</dct:identifier>");

        assertTrue(Identifier.isRelatedPublication(doc.getDocumentElement()));
    }

    @Test
    void test_is_related_publication_if_id_type_is_isbn_prefixed() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"id-type:ISBN\">\n"
                + "    10.4052/test\n"
                + "</dct:identifier>");

        assertTrue(Identifier.isRelatedPublication(doc.getDocumentElement()));
    }

    @Test
    void test_is_related_publication_if_id_type_is_issn() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"ISSN\">\n"
                + "    10.4052/test\n"
                + "</dct:identifier>");

        assertTrue(Identifier.isRelatedPublication(doc.getDocumentElement()));
    }

    @Test
    void test_is_related_publication_if_id_type_is_issn_prefixed() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"id-type:ISSN\">\n"
                + "    10.4052/test\n"
                + "</dct:identifier>");

        assertTrue(Identifier.isRelatedPublication(doc.getDocumentElement()));
    }

    @Test
    void test_is_related_publication_false_if_doi() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"id-type:DOI\">\n"
                + "    10.4052/test\n"
                + "</dct:identifier>");

        assertFalse(Identifier.isRelatedPublication(doc.getDocumentElement()));
    }

    @Test
    void test_is_related_publication_false_if_empty() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    >\n"
                + "    10.4052/test\n"
                + "</dct:identifier>");

        assertFalse(Identifier.isRelatedPublication(doc.getDocumentElement()));
    }

    @Test
    void test_is_now_grant_number_is_true_if_project_nr() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"id-type:NWO-PROJECTNR\">\n"
                + "    >\n"
                + "    10.4052/test\n"
                + "</dct:identifier>");

        assertTrue(Identifier.isNwoGrantNumber(doc.getDocumentElement()));

    }

    @Test
    void test_to_grant_number() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"id-type:NWO-PROJECTNR\">\n"
                + "    \n"
                + "    123\n"
                + "</dct:identifier>");

        var builder = new CompoundFieldBuilder("", false);
        Identifier.toNwoGrantNumber.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue()).extracting(GRANT_NUMBER_AGENCY).extracting("value")
            .containsOnly("NWO");
        assertThat(field.getValue()).extracting(GRANT_NUMBER_VALUE).extracting("value")
            .containsOnly("123");

    }

    @Test
    void test_to_related_publication_value() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"id-type:ISSN\">\n"
                + "    \n"
                + "    123\n"
                + "</dct:identifier>");

        var builder = new CompoundFieldBuilder("", false);
        Identifier.toRelatedPublicationValue.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue()).extracting(PUBLICATION_CITATION).extracting("value")
            .containsOnly("");
        assertThat(field.getValue()).extracting(PUBLICATION_ID_TYPE).extracting("value")
            .containsOnly("issn");
        assertThat(field.getValue()).extracting(PUBLICATION_ID_NUMBER).extracting("value")
            .containsOnly("123");
        assertThat(field.getValue()).extracting(PUBLICATION_URL).extracting("value")
            .containsOnly("");

    }

    @Test
    void test_to_archis_number_value() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"ARCHIS-MONUMENT\">\n"
                + "    \n"
                + "    123\n"
                + "</dct:identifier>");

        var builder = new CompoundFieldBuilder("", false);
        Identifier.toArchisNumberValue.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(field.getValue())
            .extracting(ARCHIS_NUMBER_TYPE)
            .extracting("value")
            .containsOnly("monument");

        assertThat(field.getValue())
            .extracting(ARCHIS_NUMBER_ID)
            .extracting("value")
            .containsOnly("123");
    }

    @Test
    void test_is_archis_number() throws Exception {
        var doc = readDocumentFromString(
            "<dct:identifier \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "    xsi:type=\"ARCHIS-MONUMENT\">\n"
                + "    \n"
                + "    123\n"
                + "</dct:identifier>");

        var builder = new CompoundFieldBuilder("", false);
        assertTrue(Identifier.isArchisNumber(doc.getDocumentElement()));
    }
}