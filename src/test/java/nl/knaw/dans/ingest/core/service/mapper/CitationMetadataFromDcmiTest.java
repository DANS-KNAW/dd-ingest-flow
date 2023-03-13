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

import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.dcmi;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getCompoundMultiValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getCompoundSingleValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.getPrimitiveSingleValueField;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.mapDdmToDataset;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.minimalDdmProfile;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.readDocumentFromString;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.rootAttributes;
import static nl.knaw.dans.ingest.core.service.mapper.MappingTestHelper.toPrettyJsonString;
import static org.assertj.core.api.Assertions.assertThat;

public class CitationMetadataFromDcmiTest {

    @Test
    public void CIT002_CIT010_first_title_alternatives_and_the_rest () throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi(""
            + "        <dct:title>title 1</dct:title>\n"
            + "        <dct:title>title 2</dct:title>\n"
            + "        <dct:alternative>alt title 1</dct:alternative>\n"
            + "        <dct:alternative>alt title 2</dct:alternative>")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var s = toPrettyJsonString(result);

        // CIT002 first of dcmi title/alternative
        assertThat(getPrimitiveSingleValueField("citation", "alternativeTitle", result))
            .isEqualTo("title 1");

        // CIT010 rest of dcmi title/alternative
        assertThat(getCompoundMultiValueField("citation", "dsDescription", result))
            .extracting("dsDescriptionValue").extracting("value")
            .containsExactlyInAnyOrder("<p>title 2</p>", "<p>alt title 1</p>", "<p>alt title 2</p>");
    }

    @Test
    public void CIT002A () throws Exception {
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        var field = getCompoundMultiValueField("citation", "otherId", result);
        assertThat(field).extracting("otherIdAgency").extracting("value")
            .containsExactlyInAnyOrder("otherId");
        assertThat(field).extracting("otherIdValue").extracting("value")
            .containsExactlyInAnyOrder("something");
    }

    @Test
    public void CIT027_without_series_info_in_dcmi () throws Exception {  // TODO should not produce <p></p>
        // see also DD_1292_multiple_series_informations_to_single_compound_field in MappingIntegrationMap
        var doc = readDocumentFromString(""
            + "<ddm:DDM " + rootAttributes + ">"
            + minimalDdmProfile() + dcmi("")
            + "</ddm:DDM>");
        var result = mapDdmToDataset(doc, true);
        assertThat(getCompoundSingleValueField("citation", "series", result))
            .isNull();
    }
}
