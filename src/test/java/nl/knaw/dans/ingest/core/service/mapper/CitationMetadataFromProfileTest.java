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

import org.junit.jupiter.api.Test;

import java.util.List;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.AUTHOR_NAME;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DESCRIPTION_VALUE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.DISTRIBUTION_DATE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PRODUCTION_DATE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SUBJECT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.TITLE;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.dcmi;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.ddmWithCustomProfileContent;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getCompoundMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getControlledMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveSingleValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mapDdmToDataset;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.readDocumentFromString;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.rootAttributes;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.toPrettyJsonString;
import static org.assertj.core.api.Assertions.assertThat;

public class CitationMetadataFromProfileTest {

    @Test
    void CIT001_title_maps_to_title() throws Exception{
        var doc = ddmWithCustomProfileContent("");

        var result = mapDdmToDataset(doc, false);
        assertThat(getPrimitiveSingleValueField("citation", TITLE, result))
            .isEqualTo("Title of the dataset");
    }

    @Test
    void CIT005_simple_creators_map_to_dc_creators() throws Exception{
        var doc = ddmWithCustomProfileContent(""
            + "<dc:creator>J. Bond</dc:creator>\n"
            + "<dc:creator>D. O'Seven</dc:creator>\n");

        var result = mapDdmToDataset(doc, false);
        assertThat(getCompoundMultiValueField("citation", AUTHOR, result))
            .extracting(AUTHOR_NAME).extracting("value")
            .containsExactlyInAnyOrder("J. Bond", "D. O'Seven");
    }

    @Test
    void CIT006_names_of_creatorDetails_author_map_to_author_names() throws Exception{
        var doc = ddmWithCustomProfileContent(""
            + "<dcx-dai:creatorDetails>\n"
            + "    <dcx-dai:author>\n"
            + "        <dcx-dai:surname>Bond</dcx-dai:surname>\n"
            + "    </dcx-dai:author>\n"
            + "</dcx-dai:creatorDetails>\n"
            + "<dcx-dai:creatorDetails>\n"
            + "    <dcx-dai:author>\n"
            + "        <dcx-dai:surname>O'Seven</dcx-dai:surname>\n"
            + "    </dcx-dai:author>\n"
            + "</dcx-dai:creatorDetails>\n");
        // TODO affiliation, ORCID, ISNI, DAI in AuthorTest
        var result = mapDdmToDataset(doc, false);
        assertThat(getCompoundMultiValueField("citation", AUTHOR, result))
            .extracting(AUTHOR_NAME).extracting("value")
            .containsExactlyInAnyOrder("Bond", "O'Seven");
    }

    @Test
    void CIT007_names_of_creatorDetails_organization_map_to_author_names() throws Exception{
        var doc = ddmWithCustomProfileContent(""
            + "<dcx-dai:creatorDetails>\n"
            + "    <dcx-dai:organization>\n"
            + "        <dcx-dai:name xml:lang='en'>DANS</dcx-dai:name>\n"
            + "    </dcx-dai:organization>\n"
            + "</dcx-dai:creatorDetails>\n"
            + "<dcx-dai:creatorDetails>\n"
            + "    <dcx-dai:organization>\n"
            + "        <dcx-dai:name xml:lang='en'>KNAW</dcx-dai:name>\n"
            + "    </dcx-dai:organization>\n"
            + "</dcx-dai:creatorDetails>\n");
        // TODO affiliation, ISNI, VIAF in AuthorTest
        var result = mapDdmToDataset(doc, false);
        assertThat(getCompoundMultiValueField("citation", AUTHOR, result))
            .extracting(AUTHOR_NAME).extracting("value")
            .containsExactlyInAnyOrder("DANS", "KNAW");
    }

    @Test
    void CIT009_descriptions_map_to_descriptions() throws Exception{
        var doc = ddmWithCustomProfileContent(""
            + "<dc:description>Lorem ipsum.</dc:description>\n"
            + "<dc:description>dolor sit amet</dc:description>\n"
        );

        var result = mapDdmToDataset(doc, false);
        assertThat(getCompoundMultiValueField("citation", DESCRIPTION, result))
            .extracting(DESCRIPTION_VALUE).extracting("value")
            .containsExactlyInAnyOrder("<p>Lorem ipsum.</p>", "<p>dolor sit amet</p>");
    }

    @Test
    void CIT009_description_type_technical_info_maps_once_to_description_DD_1216() throws Exception {
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
    void CIT013_subject_maps_to__subject_without_other_DD_1265() throws Exception {
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
    void CIT013_subject_maps_only_to_other_DD_1265() throws Exception {
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
    void CIT019_creation_date_maps_to_production_date() throws Exception{
        var doc = ddmWithCustomProfileContent("<ddm:created>2012-12</ddm:created>");

        var result = mapDdmToDataset(doc, false);
        assertThat(getPrimitiveSingleValueField("citation", PRODUCTION_DATE, result))
            .isEqualTo("2012-12-01");
    }

    @Test
    void CIT025_date_available_maps_to_distribution_date() throws Exception{
        var doc = ddmWithCustomProfileContent("<ddm:available>2014-12</ddm:available>");

        var result = mapDdmToDataset(doc, false);
        assertThat(getPrimitiveSingleValueField("citation", DISTRIBUTION_DATE, result))
            .isEqualTo("2014-12-01");
    }
}
