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

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_TEXT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_URI;
import static org.assertj.core.api.Assertions.assertThat;

class RelationTest extends BaseTest {

    @Test
    void test_to_relation_object() throws Exception {
        // TODO inline XML
        var doc = readDocument("dataset.xml");
        var items = XPathEvaluator.nodes(doc, "//ddm:dcmiMetadata//*")
            .filter(Relation::isRelation)
            .map(Relation::toRelationObject)
            .collect(Collectors.toList());

        assertThat(items)
            .extracting(RELATION)
            .extracting("value")
            .containsOnly("relation", "conforms to");

        assertThat(items)
            .extracting(RELATION_URI)
            .extracting("value")
            .containsOnly("", "https://knaw.nl/");

        assertThat(items)
            .extracting(RELATION_TEXT)
            .extracting("value")
            .containsOnly("Relation", "Conforms To");
    }
}