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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InCollectionTest extends BaseTest {

    @Test
    void term_uri_correctly_extracted() throws Exception {
        var valueUri = "https://vocabularies.dans.knaw.nl/collections/ssh/7e30e380-9e02-4b15-a0af-f35286c3ecec";
        var doc = readDocumentFromString(String.format(
            "<ddm:inCollection"
                + "  xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\" \n"
                + "  subjectScheme=\"DANS Collection\" \n"
                + "  valueURI=\"%s\" \n"
                + "  schemeURI=\"https://vocabularies.dans.knaw.nl/collections\">CLARIN</ddm:inCollection>", valueUri));

        assertThat(InCollection.toCollection(doc.getDocumentElement())).isEqualTo(valueUri);
    }

    @Test
    void missing_term_uri_throws_exception() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:inCollection"
                + "  xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\" \n"
                + "  subjectScheme=\"DANS Collection\" \n"
                + "  schemeURI=\"https://vocabularies.dans.knaw.nl/collections\">CLARIN</ddm:inCollection>");

        var thrown = assertThrows(RuntimeException.class, () -> InCollection.toCollection(doc.getDocumentElement()));
        assertThat(thrown.getMessage()).contains("Missing attribute valueURI");
    }

}
