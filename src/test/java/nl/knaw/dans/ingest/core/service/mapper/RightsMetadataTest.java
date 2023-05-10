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

import nl.knaw.dans.ingest.core.exception.MissingRequiredFieldException;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.LANGUAGE_OF_METADATA;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.PERSONAL_DATA_PRESENT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RIGHTS_HOLDER;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.dcmi;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getControlledMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getControlledSingleValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mapDdmToDataset;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.minimalDdmProfile;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.readDocumentFromString;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.rootAttributes;
import static org.assertj.core.api.Assertions.assertThat;

class RightsMetadataTest {

    RightsMetadataTest() throws Exception {
    }

    private final Document ddmWithOrganizations = readDocumentFromString(""
        + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>\n"
        + minimalDdmProfile()
        + "    <ddm:dcmiMetadata>\n"
        + "        <dcx-dai:contributorDetails>\n"
        + "            <dcx-dai:organization>\n"
        + "                <dcx-dai:role>RightsHolder</dcx-dai:role>\n"
        + "                <dcx-dai:name>Some org</dcx-dai:name>\n"
        + "            </dcx-dai:organization>\n"
        + "        </dcx-dai:contributorDetails>\n\n"
        + "        <dcx-dai:contributorDetails>\n"
        + "            <dcx-dai:organization>\n"
        + "                <dcx-dai:role>RightsHolder</dcx-dai:role>\n"
        + "                <dcx-dai:name xml:lang='de'>Some other org</dcx-dai:name>\n"
        + "            </dcx-dai:organization>\n"
        + "        </dcx-dai:contributorDetails>\n\n"
        + "        <dcx-dai:contributorDetails>\n"
        + "            <dcx-dai:organization xml:lang='dut'>\n"
        + "                <dcx-dai:name>Something different</dcx-dai:name>\n"
        + "            </dcx-dai:organization>\n"
        + "        </dcx-dai:contributorDetails>\n\n"
        + "    </ddm:dcmiMetadata>\n"
        + "</ddm:DDM>\n");

    @Test
    void RIG000A_organizations_with_role_rightsHolder_map_to_rights_holder() {

        var result = mapDdmToDataset(ddmWithOrganizations, false);
        assertThat(getPrimitiveMultiValueField("dansRights", RIGHTS_HOLDER, result))
            .containsOnly("Some org", "Some other org");
    }

    @Test
    void RIG003_lang_attributes_of_organizations_map_to_language_of_metadata() {

        var result = mapDdmToDataset(ddmWithOrganizations, false);
        assertThat(getControlledMultiValueField("dansRights", LANGUAGE_OF_METADATA, result))
            .containsOnly("Dutch", "German");
    }

    private final Document ddmWithAuthors = readDocumentFromString(""
        + "<ddm:DDM " + rootAttributes + " xmlns:dcx-dai='http://easy.dans.knaw.nl/schemas/dcx/dai/'>\n"
        + minimalDdmProfile()
        + "    <ddm:dcmiMetadata>\n"
        + "        <dcx-dai:contributorDetails>\n"
        + "            <dcx-dai:author xml:lang='nl'>\n"
        + "                <dcx-dai:role>RightsHolder</dcx-dai:role>\n"
        + "                <dcx-dai:organization>\n"
        + "                    <dcx-dai:name>Example Org</dcx-dai:name>\n"
        + "                </dcx-dai:organization>\n"
        + "            </dcx-dai:author>\n"
        + "        </dcx-dai:contributorDetails>\n\n"
        + "        <dcx-dai:contributorDetails xml:lang='ger'>\n"
        + "            <dcx-dai:author>\n"
        + "                <dcx-dai:role>RightsHolder</dcx-dai:role>\n"
        + "                <dcx-dai:organization>\n"
        + "                    <dcx-dai:name>Another Org</dcx-dai:name>\n"
        + "                </dcx-dai:organization>\n"
        + "            </dcx-dai:author>\n"
        + "        </dcx-dai:contributorDetails>\n\n"
        + "    </ddm:dcmiMetadata>\n"
        + "</ddm:DDM>\n");

    @Test
    void RIG000B_authors_with_role_rightsHolder_and_organization_map_to_rights_holders() {

        var result = mapDdmToDataset(ddmWithAuthors, false);
        assertThat(getPrimitiveMultiValueField("dansRights", RIGHTS_HOLDER, result))
            .containsOnly("Example Org", "Another Org");
    }

    @Test
    void RIG003_lang_attributes_of_authors_with_role_rightsHolder_and_organization_map_to_language_of_metadata() {

        var result = mapDdmToDataset(ddmWithAuthors, false);
        assertThat(getControlledMultiValueField("dansRights", LANGUAGE_OF_METADATA, result))
            .containsOnly("Dutch", "German");
    }

    @Test
    void RIG001_dct_rights_holders_map_to_rights_holders() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + minimalDdmProfile()
            + "    <ddm:dcmiMetadata>\n"
            + "        <dct:rightsHolder>James Bond</dct:rightsHolder>\n"
            + "        <dct:rightsHolder>Double O'Seven</dct:rightsHolder>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);
        assertThat(getPrimitiveMultiValueField("dansRights", RIGHTS_HOLDER, result))
            .containsOnly("James Bond", "Double O'Seven");
    }

    @Test
    void RIG003_lang_attributes_of_plain_rightHolders_map_to_language_of_metadata() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + minimalDdmProfile()
            + "    <ddm:dcmiMetadata>\n"
            + "        <dct:rightsHolder xml:lang='dut'>James Bond</dct:rightsHolder>\n"
            + "        <dct:rightsHolder xml:lang='nl'>Double O'Seven</dct:rightsHolder>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);
        assertThat(getControlledMultiValueField("dansRights", LANGUAGE_OF_METADATA, result))
            .containsOnly("Dutch");
    }

    @Test
    void RIG00N_rights_holder_must_be_present() throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + minimalDdmProfile()
            + "</ddm:DDM>\n");
        try {
            mapDdmToDataset(doc, false);
        }
        catch (MissingRequiredFieldException e) {
            assertThat(e.getMessage()).containsOnlyOnce("dansRightsHolder");
        }
    }

    @Test
    void RIG002_no_ddm_personalData_maps_to_default() throws Exception {
        // required in DDM, validated by v2-schema
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + minimalDdmProfile()
            + dcmi("")
            + "</ddm:DDM>\n");

        var result = mapDdmToDataset(doc, false);
        assertThat(getControlledSingleValueField("dansRights", PERSONAL_DATA_PRESENT, result))
            .isEqualTo("Unknown");
    }

    @Test
    void RIG002_ddm_personalData_maps_to_personal_data_present() throws Exception {
        // required in DDM, validated by v2-schema
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + "    <ddm:profile>\n"
            + "        <dc:title>Title of the dataset</dc:title>\n"
            + "        <ddm:audience>D19200</ddm:audience>\n"
            + "        <ddm:personalData present='Yes'></ddm:personalData>\n"
            + "    </ddm:profile>\n"
            + dcmi("")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        assertThat(getControlledSingleValueField("dansRights", PERSONAL_DATA_PRESENT, result))
            .isEqualTo("Yes");
        // "No" is tested in the PersonalDataTest class
    }

    @Test
    void RIG002_ddm_personalData_without_attribute_maps_to_default() throws Exception {
        // required in DDM, validated by v2-schema
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">\n"
            + "    <ddm:profile>\n"
            + "        <dc:title>Title of the dataset</dc:title>\n"
            + "        <ddm:audience>D19200</ddm:audience>\n"
            + "        <ddm:personalData></ddm:personalData>\n"
            + "    </ddm:profile>\n"
            + dcmi("")
            + "</ddm:DDM>");

        var result = mapDdmToDataset(doc, false);
        assertThat(getControlledSingleValueField("dansRights", PERSONAL_DATA_PRESENT, result))
            .isEqualTo("Unknown");
    }
}