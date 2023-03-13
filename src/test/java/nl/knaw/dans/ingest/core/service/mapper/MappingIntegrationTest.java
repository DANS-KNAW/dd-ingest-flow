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
package nl.knaw.dans.ingest.core.service.mapper;

import nl.knaw.dans.lib.dataverse.model.dataset.CompoundSingleValueField;
import org.junit.jupiter.api.Test;

import java.util.List;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.NOTES_TEXT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SERIES;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SERIES_INFORMATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SUBJECT;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.dcmi;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getCompoundMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getControlledMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveSingleValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mapDdmToDataset;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.minimalDdmProfile;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.readDocumentFromString;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.rootAttributes;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.toPrettyJsonString;
import static org.assertj.core.api.Assertions.assertThat;

class MappingIntegrationTest {

    @Test
    void DD_1216_description_type_other_maps_only_to_author_name() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + minimalDdmProfile()
            + dcmi("<ddm:description descriptionType=\"Other\">Author from description other</ddm:description>\n")
            + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);
        var field = getCompoundMultiValueField("citation", "contributor", result);
        var expected = "Author from description other";
        assertThat(field).extracting(CONTRIBUTOR_NAME).extracting("value")
            .containsOnly(expected);
        // not as description and author
        assertThat(toPrettyJsonString(result)).containsOnlyOnce(expected);
    }

    @Test
    void DD_1216_description_type_technical_info_maps_once_to_description() throws Exception {
        String dcmiContent = ""
            + "<dct:description>plain description</dct:description>\n"
            + "<ddm:description descriptionType=\"TechnicalInfo\">technical description</ddm:description>\n"
            + "<ddm:description descriptionType=\"NotKnown\">not known description type</ddm:description>\n";
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + "    <ddm:profile>\n"
            + "        <dc:title>Title of the dataset</dc:title>\n"
            + "        <dc:description>Lorem ipsum.</dc:description>\n"
            + "        <ddm:audience>D24000</ddm:audience>"
            + "    </ddm:profile>\n"
            + dcmi(dcmiContent)
            + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);
        var str = toPrettyJsonString(result);
        assertThat(str).containsOnlyOnce("not known description type");
        assertThat(str).containsOnlyOnce("technical description");
        assertThat(str).containsOnlyOnce("Lorem ipsum");
        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly("<p>plain description</p>", "<p>Lorem ipsum.</p>", "<p>technical description</p>", "<p>not known description type</p>");
    }

    @Test
    void DD_1292_multiple_series_informations_to_single_compound_field() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:DDM " + rootAttributes + ">\n"
                + minimalDdmProfile()
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <ddm:description descriptionType=\"SeriesInformation\">series\n123</ddm:description>\n"
                + "        <ddm:description descriptionType=\"SeriesInformation\">another\nseries\n456</ddm:description>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);

        var field = (CompoundSingleValueField) result.getDatasetVersion().getMetadataBlocks()
            .get("citation").getFields().stream()
            .filter(f -> f.getTypeName().equals(SERIES)).findFirst().orElseThrow();
        assertThat(field.getValue())
            .extracting(SERIES_INFORMATION )
            .extracting("value")
            .isEqualTo("<p>series<br>123</p><p>another<br>series<br>456</p>");
    }

    @Test
    void DD_1216_provenance_maps_to_notes() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + minimalDdmProfile()
            + dcmi("<dct:provenance>copied xml to csv</dct:provenance>\n")
            + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);
        var str = toPrettyJsonString(result);

        assertThat(str).containsOnlyOnce("copied xml to csv");
        assertThat(str).doesNotContain("<p>copied xml to csv</p>");

        assertThat(getPrimitiveSingleValueField("citation", NOTES_TEXT, result))
            .isEqualTo("copied xml to csv");
    }

    @Test
    void DD_1265_subject_omits_other() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + "    <ddm:profile>\n"
            + "        <dc:title>Title of the dataset</dc:title>\n"
            + "        <ddm:audience>D19200</ddm:audience>"
            + "        <ddm:audience>D11200</ddm:audience>"
            + "        <ddm:audience>D88200</ddm:audience>"
            + "        <ddm:audience>D40200</ddm:audience>"
            + "        <ddm:audience>D17200</ddm:audience>"
            + "    </ddm:profile>\n"
            + dcmi("")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        assertThat(getControlledMultiValueField("citation", SUBJECT, result))
            .isEqualTo(List.of("Astronomy and Astrophysics", "Law", "Mathematical Sciences"));
    }

    @Test
    void DD_1265_subject_is_other() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + "    <ddm:profile>\n"
            + "        <dc:title xml:lang='en'>Title of the dataset</dc:title>\n"
            + "        <ddm:audience>D19200</ddm:audience>"
            + "        <ddm:audience>D88200</ddm:audience>"
            + "    </ddm:profile>\n"
            + dcmi("")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        assertThat(getControlledMultiValueField("citation", SUBJECT, result))
            .isEqualTo(List.of("Other"));
    }

    @Test
    void DD_1216_DctAccesRights_maps_to_description() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + "    <ddm:profile>\n"
            + "        <dc:title>Title of the dataset</dc:title>\n"
            + "        <dc:description>Lorem ipsum.</dc:description>\n"
            + "        <ddm:audience>D24000</ddm:audience>"
            + "    </ddm:profile>\n"
            + dcmi("<dct:accessRights>Some story</dct:accessRights>\n")
            + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);
        var str = toPrettyJsonString(result);

        assertThat(str).containsOnlyOnce("<p>Some story</p>");

        var field = getCompoundMultiValueField("citation", DESCRIPTION, result);
        assertThat(field).extracting(DESCRIPTION_VALUE).extracting("value")
            .containsOnly("<p>Some story</p>", "<p>Lorem ipsum.</p>");
        assertThat(result.getDatasetVersion().getTermsOfAccess()).isEqualTo("");
    }
}