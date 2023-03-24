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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InCollectionTest extends BaseTest {

    @Test
    void toCollection_term_uri_correctly_extracted() throws Exception {
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
    void toCollection_should_throw_exception_when_termuri_is_missing() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:inCollection"
                + "  xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\" \n"
                + "  subjectScheme=\"DANS Collection\" \n"
                + "  schemeURI=\"https://vocabularies.dans.knaw.nl/collections\">CLARIN</ddm:inCollection>");

        var thrown = assertThrows(RuntimeException.class, () -> InCollection.toCollection(doc.getDocumentElement()));
        assertThat(thrown.getMessage()).contains("Missing attribute valueURI");
    }

    @Test
    void isCollection_should_return_true_when_values_match_expected() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:inCollection"
                + "  xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\" \n"
                + "  subjectScheme=\"DANS Collection\" \n"
                + "  schemeURI=\"https://vocabularies.dans.knaw.nl/collections\">CLARIN</ddm:inCollection>");

        assertTrue(InCollection.isCollection(doc.getDocumentElement()));
    }

    @Test
    void isCollection_should_return_false_when_subjectScheme_is_different() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:inCollection"
                + "  xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\" \n"
                + "  subjectScheme=\"DANS Collection 2\" \n"
                + "  schemeURI=\"https://vocabularies.dans.knaw.nl/collections\">CLARIN</ddm:inCollection>");

        assertFalse(InCollection.isCollection(doc.getDocumentElement()));
    }

    @Test
    void isCollection_should_return_false_when_subjectSchemeURI_is_different() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:inCollection"
                + "  xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\" \n"
                + "  subjectScheme=\"DANS Collection\" \n"
                + "  schemeURI=\"https://vocabularies.dans.knaw.nl/different-collections\">CLARIN</ddm:inCollection>");

        assertFalse(InCollection.isCollection(doc.getDocumentElement()));
    }
}
