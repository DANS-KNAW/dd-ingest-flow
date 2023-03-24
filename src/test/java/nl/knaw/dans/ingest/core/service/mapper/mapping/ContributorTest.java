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

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.CONTRIBUTOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContributorTest extends BaseTest {

    @Test
    void isValidContributer_should_return_true_for_contributorDetails_with_dcx_author() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:contributorDetails xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "    <dcx-dai:author>\n"
            + "        <dcx-dai:titles>Dhr</dcx-dai:titles>\n"
            + "        <dcx-dai:initials>I</dcx-dai:initials>\n"
            + "        <dcx-dai:insertions>de</dcx-dai:insertions>\n"
            + "        <dcx-dai:surname>Lastname</dcx-dai:surname>\n"
            + "        <dcx-dai:organization>\n"
            + "            <dcx-dai:name xml:lang=\"en\">Example Org</dcx-dai:name>\n"
            + "        </dcx-dai:organization>\n"
            + "    </dcx-dai:author>\n"
            + "</dcx-dai:contributorDetails>");

        assertTrue(Contributor.isValidContributor(doc.getDocumentElement()));
    }

    @Test
    void toContributorValueObject_should_return_correct_contributor_name() throws Exception {
        var doc = readDocumentFromString("<dcx-dai:contributorDetails xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
            + "    <dcx-dai:author>\n"
            + "        <dcx-dai:titles>Dhr</dcx-dai:titles>\n"
            + "        <dcx-dai:initials>I</dcx-dai:initials>\n"
            + "        <dcx-dai:insertions>de</dcx-dai:insertions>\n"
            + "        <dcx-dai:surname>Lastname</dcx-dai:surname>\n"
            + "        <dcx-dai:organization>\n"
            + "            <dcx-dai:name xml:lang=\"en\">Example Org</dcx-dai:name>\n"
            + "        </dcx-dai:organization>\n"
            + "    </dcx-dai:author>\n"
            + "</dcx-dai:contributorDetails>");

        var builder = new CompoundFieldBuilder("", true);
        Contributor.toContributorValueObject.build(builder, doc.getDocumentElement());
        var field = builder.build();

        assertThat(((CompoundMultiValueField)field).getValue())
            .extracting(CONTRIBUTOR_NAME)
            .extracting("value")
            .containsOnly("I de Lastname (Example Org)");
    }
}