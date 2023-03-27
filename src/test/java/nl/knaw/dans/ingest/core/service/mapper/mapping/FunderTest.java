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

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_AGENCY;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.GRANT_NUMBER_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FunderTest extends BaseTest {

    @Test
    void parseFunderDetails_should_parse_correctly() throws Exception {
        var xml = readDocumentFromString("<ddm:funding xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "            <ddm:funderName>Funder name</ddm:funderName>\n"
            + "            <ddm:fundingProgramme>Funding programme</ddm:fundingProgramme>\n"
            + "            <ddm:awardNumber>Award number</ddm:awardNumber>\n"
            + "            <ddm:awardTitle xml:lang=\"en\">Award title</ddm:awardTitle>\n"
            + "        </ddm:funding>");

        var result = Funder.parseFunderDetails(xml.getDocumentElement());
        assertEquals("Funder name", result.getFunderName());
        assertEquals("Funding programme", result.getFundingProgramme());
        assertEquals("Award title", result.getAwardTitle());
        assertEquals("Award number", result.getAwardNumber());
    }

    @Test
    void toGrantNumberValueObject_should_generate_result() throws Exception {
        var xml = readDocumentFromString("<ddm:funding xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\">\n"
            + "            <ddm:funderName>Funder name</ddm:funderName>\n"
            + "            <ddm:fundingProgramme>Funding programme</ddm:fundingProgramme>\n"
            + "            <ddm:awardNumber>Award number</ddm:awardNumber>\n"
            + "            <ddm:awardTitle xml:lang=\"en\">Award title</ddm:awardTitle>\n"
            + "        </ddm:funding>");

        var builder = new CompoundFieldBuilder("", true);
        Funder.toGrantNumberValueObject.build(builder, xml.getDocumentElement());
        var field = builder.build();

        assertThat(((CompoundMultiValueField) field).getValue())
            .extracting(GRANT_NUMBER_AGENCY)
            .extracting("value")
            .containsOnly("Funder name");

        assertThat(((CompoundMultiValueField) field).getValue())
            .extracting(GRANT_NUMBER_VALUE)
            .extracting("value")
            .containsOnly("Funding programme Award number");
    }
}