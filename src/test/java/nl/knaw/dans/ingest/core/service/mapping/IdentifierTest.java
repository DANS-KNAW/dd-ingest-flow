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

import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.ARCHIS_NUMBER_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

class IdentifierTest extends BaseTest {

    @Test
    void testToArchisNumberValue() throws Exception {
        var ddm = readDocument("dataset.xml");
        var ids = XPathEvaluator.nodes(ddm, "//ddm:dcmiMetadata/dcterms:identifier");

        var results = ids
            .filter(Identifier::isArchisZaakId)
            .map(Identifier::toArchisNumberValue)
            .collect(Collectors.toList());

        assertThat(results)
            .extracting(ARCHIS_NUMBER_TYPE)
            .extracting("value")
            .containsOnly("monument");

    }
}