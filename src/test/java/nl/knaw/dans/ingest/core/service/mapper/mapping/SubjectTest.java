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

import static nl.knaw.dans.ingest.core.service.mapper.mapping.Subject.SCHEME_AAT;
import static nl.knaw.dans.ingest.core.service.mapper.mapping.Subject.SCHEME_PAN;
import static nl.knaw.dans.ingest.core.service.mapper.mapping.Subject.SCHEME_URI_AAT;
import static nl.knaw.dans.ingest.core.service.mapper.mapping.Subject.SCHEME_URI_PAN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubjectTest extends BaseTest {

    @Test
    void hasNoCvAttributes_should_return_false_for_ABR_subjects() throws Exception {
        var doc = readDocumentFromString("<ddm:subject xml:lang=\"nl\" \n"
            + "    xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "    valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/2179d872-888f-4807-a6d5-5e5afaa616c4\" \n"
            + "    subjectScheme=\"ABR Complextypen\" \n"
            + "    schemeURI=\"https://data.cultureelerfgoed.nl/term/id/abr/e9546020-4b28-4819-b0c2-29e7c864c5c0\">\n"
            + "    bewoning (inclusief verdediging)\n"
            + "</ddm:subject>");

        assertFalse(Subject.hasNoCvAttributes(doc.getDocumentElement()));
    }

    @Test
    void hasNoCvAttributes_should_return_true_for_a_subject_with_no_subjectScheme_and_schemeURI_attributes() throws Exception {
        var doc = readDocumentFromString("<ddm:subject xml:lang=\"nl\" \n"
            + "    xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "    >\n"
            + "    bewoning (inclusief verdediging)\n"
            + "</ddm:subject>");

        assertTrue(Subject.hasNoCvAttributes(doc.getDocumentElement()));
    }

    @Test
    void isPanTerm_should_return_true_if_both_schemeURI_and_subjectScheme_match() throws Exception {
        var doc = readDocumentFromString(String.format(
            "<ddm:subject xml:lang=\"nl\" \n"
                + "    xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "    valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/2179d872-888f-4807-a6d5-5e5afaa616c4\" \n"
                + "    subjectScheme=\"%s\" \n"
                + "    schemeURI=\"%s\">\n"
                + "    bewoning (inclusief verdediging)\n"
                + "</ddm:subject>", SCHEME_PAN, SCHEME_URI_PAN));

        assertTrue(Subject.isPanTerm(doc.getDocumentElement()));
    }

    @Test
    void isPanTerm_should_return_false_if_schemeURI_does_not_match_for_pan() throws Exception {
        var doc = readDocumentFromString(String.format(
            "<ddm:subject xml:lang=\"nl\" \n"
                + "    xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "    valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/2179d872-888f-4807-a6d5-5e5afaa616c4\" \n"
                + "    subjectScheme=\"%s\" \n"
                + "    schemeURI=\"%s\">\n"
                + "    bewoning (inclusief verdediging)\n"
                + "</ddm:subject>", SCHEME_PAN, "https://not-pan"));

        assertFalse(Subject.isPanTerm(doc.getDocumentElement()));
    }

    @Test
    void isPanTerm_should_return_false_if_subjectScheme_does_not_match() throws Exception {
        var doc = readDocumentFromString(String.format(
            "<ddm:subject xml:lang=\"nl\" \n"
                + "    xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "    valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/2179d872-888f-4807-a6d5-5e5afaa616c4\" \n"
                + "    subjectScheme=\"%s\" \n"
                + "    schemeURI=\"%s\">\n"
                + "    bewoning (inclusief verdediging)\n"
                + "</ddm:subject>", "NOT PAN", SCHEME_URI_PAN));

        assertFalse(Subject.isPanTerm(doc.getDocumentElement()));
    }

    @Test
    void isAatTerm_should_return_true_if_both_schemeURI_and_subjectScheme_match() throws Exception {
        var doc = readDocumentFromString(String.format(
            "<ddm:subject xml:lang=\"nl\" \n"
                + "    xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "    valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/2179d872-888f-4807-a6d5-5e5afaa616c4\" \n"
                + "    subjectScheme=\"%s\" \n"
                + "    schemeURI=\"%s\">\n"
                + "    bewoning (inclusief verdediging)\n"
                + "</ddm:subject>", SCHEME_AAT, SCHEME_URI_AAT));

        assertTrue(Subject.isAatTerm(doc.getDocumentElement()));
    }

    @Test
    void isAatTerm_should_return_false_if_schemeURI_does_not_match() throws Exception {
        var doc = readDocumentFromString(String.format(
            "<ddm:subject xml:lang=\"nl\" \n"
                + "    xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "    valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/2179d872-888f-4807-a6d5-5e5afaa616c4\" \n"
                + "    subjectScheme=\"%s\" \n"
                + "    schemeURI=\"%s\">\n"
                + "    bewoning (inclusief verdediging)\n"
                + "</ddm:subject>", SCHEME_AAT, "https://not-aat"));

        assertFalse(Subject.isAatTerm(doc.getDocumentElement()));
    }

    @Test
    void isAatTerm_should_return_false_if_subjectScheme_does_not_match() throws Exception {
        var doc = readDocumentFromString(String.format(
            "<ddm:subject xml:lang=\"nl\" \n"
                + "    xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "    valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/2179d872-888f-4807-a6d5-5e5afaa616c4\" \n"
                + "    subjectScheme=\"%s\" \n"
                + "    schemeURI=\"%s\">\n"
                + "    bewoning (inclusief verdediging)\n"
                + "</ddm:subject>", "NOT AAT", SCHEME_URI_AAT));

        assertFalse(Subject.isAatTerm(doc.getDocumentElement()));
    }

    @Test
    void removeMatchPrefix_should_remove_Broader_Match_before_a_term() throws Exception {
        assertEquals("buttons (fasteners)",
            Subject.removeMatchPrefix("Broader Match: buttons (fasteners)"));
    }

    @Test
    void removeMatchPrefix_should_remove_Close_Match_before_a_term() throws Exception {
        assertEquals("buttons (fasteners)",
            Subject.removeMatchPrefix("Close Match: buttons (fasteners)"));
    }

    @Test
    void removeMatchPrefix_should_term_unchanged_if_it_has_no_prefix() throws Exception {
        assertEquals("buttons (fasteners)",
            Subject.removeMatchPrefix("buttons (fasteners)"));
    }

}