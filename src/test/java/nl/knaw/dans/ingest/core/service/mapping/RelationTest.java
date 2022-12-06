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
import nl.knaw.dans.lib.dataverse.CompoundFieldBuilder;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_TEXT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_TYPE;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.RELATION_URI;
import static org.assertj.core.api.Assertions.assertThat;

class RelationTest extends BaseTest {

    @Test
    void test_to_relation_object() throws Exception {

        var doc = readDocumentFromString(
            "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "         xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/md/ddm/ http://easy.dans.knaw.nl/schemas/md/2017/09/ddm.xsd\">\n"
                + "    <ddm:dcmiMetadata>\n"
                + "        <dct:license xsi:type=\"dct:URI\">http://creativecommons.org/licenses/by-sa/4.0</dct:license>\n"
                + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
                + "        <ddm:relation href=\"https://knaw.nl/\">Relation</ddm:relation>\n"
                + "        <ddm:conformsTo>Conforms To</ddm:conformsTo>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>\n");

        var builder = new CompoundFieldBuilder("", true);

        XPathEvaluator.nodes(doc, "//ddm:dcmiMetadata//*")
            .filter(Relation::isRelation)
            .forEach(item -> {
                Relation.toRelationObject.build(builder, item);
                builder.nextValue();
            });

        var result = builder.build();
        // the last call to nextValue creates an empty record, filter it out
        var list = result.getValue()
            .stream()
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());

        assertThat(list)
            .extracting(RELATION_TYPE)
            .extracting("value")
            .containsOnly("relation", "conforms to");

        assertThat(list)
            .extracting(RELATION_URI)
            .extracting("value")
            .containsOnly("", "https://knaw.nl/");

        assertThat(list)
            .extracting(RELATION_TEXT)
            .extracting("value")
            .containsOnly("Relation", "Conforms To");
    }
}