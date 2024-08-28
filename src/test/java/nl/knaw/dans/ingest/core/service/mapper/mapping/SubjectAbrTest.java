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

import java.util.Map;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_ARTIFACT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_COMPLEX;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_OLD;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_ARTIFACT;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_COMPLEX;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_OLD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubjectAbrTest extends BaseTest {

    @Test
    void isOldAbr_should_return_true_if_schemeURI_of_old_ABR_is_used_and_subjectScheme_matches_name() throws Exception {
        var doc = readDocumentFromString(String.format(
            """
                <ddm:subject xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    schemeURI="%s"
                    subjectScheme="%s"
                    valueURI="https://test4.com/supersecret/"
                    >
                    ABR BASIS REGISTER OLD
                </ddm:subject>""",
            SCHEME_URI_ABR_OLD, SCHEME_ABR_OLD
        ));

        assertTrue(SubjectAbr.isOldAbr(doc.getDocumentElement()));
    }

    @Test
    void isOldAbr_should_return_false_if_schemeURI_does_not_match() throws Exception {
        var doc = readDocumentFromString(String.format(
            """
                <ddm:subject xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    schemeURI="%s"
                    subjectScheme="%s"
                    valueURI="https://test4.com/supersecret/"
                    >
                    ABR BASIS REGISTER OLD
                </ddm:subject>""",
            "http://notabr", SCHEME_ABR_OLD
        ));

        assertFalse(SubjectAbr.isOldAbr(doc.getDocumentElement()));
    }

    @Test
    void isOldAbr_should_return_false_if_subjectScheme_does_not_match() throws Exception {
        var doc = readDocumentFromString(String.format(
            """
                <ddm:subject xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    schemeURI="%s"
                    subjectScheme="%s"
                    valueURI="https://test4.com/supersecret/"
                    >
                    ABR BASIS REGISTER OLD
                </ddm:subject>""",
            SCHEME_URI_ABR_OLD, "NO MATCH"
        ));

        assertFalse(SubjectAbr.isOldAbr(doc.getDocumentElement()));
    }

    @Test
    void isAbrArtifact_should_return_true_for_subject_element_matching_schemeURI_and_subjectScheme_attributes() throws Exception {
        var doc = readDocumentFromString(String.format(
            """
                <ddm:subject xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    schemeURI="%s"
                    subjectScheme="%s"
                    valueURI="https://test4.com/supersecret/"
                    >
                    ABR BASIS REGISTER OLD
                </ddm:subject>""",
            SCHEME_URI_ABR_ARTIFACT, SCHEME_ABR_ARTIFACT
        ));

        assertTrue(SubjectAbr.isAbrArtifact(doc.getDocumentElement()));
    }

    @Test
    void isAbrArtifact_should_return_false_for_wrong_schemeURI() throws Exception {
        var doc = readDocumentFromString(String.format(
            """
                <ddm:subject xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    schemeURI="%s"
                    subjectScheme="%s"
                    valueURI="https://test4.com/supersecret/"
                    >
                    ABR BASIS REGISTER OLD
                </ddm:subject>""",
            "https://fake", SCHEME_ABR_ARTIFACT
        ));

        assertFalse(SubjectAbr.isAbrArtifact(doc.getDocumentElement()));
    }

    @Test
    void isAbrArtifact_should_return_false_for_wrong_subjectScheme() throws Exception {
        var doc = readDocumentFromString(String.format(
            """
                <ddm:subject xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    schemeURI="%s"
                    subjectScheme="%s"
                    valueURI="https://test4.com/supersecret/"
                    >
                    ABR BASIS REGISTER OLD
                </ddm:subject>""",
            SCHEME_URI_ABR_ARTIFACT, "NO MATCH"
        ));

        assertFalse(SubjectAbr.isAbrArtifact(doc.getDocumentElement()));
    }

    @Test
    void isAbrComplex_should_return_true_for_subject_element_matching_schemeURI_and_subjectScheme_attributes() throws Exception {
        var doc = readDocumentFromString(String.format(
            """
                <ddm:subject xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    schemeURI="%s"
                    subjectScheme="%s"
                    valueURI="https://test4.com/supersecret/"
                    >
                    ABR BASIS REGISTER OLD
                </ddm:subject>""",
            SCHEME_URI_ABR_COMPLEX, SCHEME_ABR_COMPLEX
        ));

        assertTrue(SubjectAbr.isAbrComplex(doc.getDocumentElement()));
    }

    @Test
    void isAbrComplex_should_return_false_for_wrong_schemeURI() throws Exception {
        var doc = readDocumentFromString(String.format(
            """
                <ddm:subject xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    schemeURI="%s"
                    subjectScheme="%s"
                    valueURI="https://test4.com/supersecret/"
                    >
                    ABR BASIS REGISTER OLD
                </ddm:subject>""",
            "https://fake", SCHEME_ABR_COMPLEX
        ));

        assertFalse(SubjectAbr.isAbrComplex(doc.getDocumentElement()));
    }

    @Test
    void isAbrComplex_should_return_false_for_wrong_subjectScheme() throws Exception {
        var doc = readDocumentFromString(String.format(
            """
                <ddm:subject xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    schemeURI="%s"
                    subjectScheme="%s"
                    valueURI="https://test4.com/supersecret/"
                    >
                    ABR BASIS REGISTER OLD
                </ddm:subject>""",
            SCHEME_URI_ABR_COMPLEX, "NO MATCH"
        ));

        assertFalse(SubjectAbr.isAbrComplex(doc.getDocumentElement()));
    }

    @Test
    void toAbrComplex_should_create_new_termURI_from_legacy_URI_by_using_the_UUID_from_the_legacy_URI() throws Exception {
        var legacyBaseUrl = "https://data.cultureelerfgoed.nl/term/id/rn";
        var abrBaseUrl = "https://data.cultureelerfgoed.nl/term/id/abr";
        var termUuid = "ea77d56e-1475-4e4c-94f5-489bd3d9a3e7";
        var valueUri = legacyBaseUrl + "/" + termUuid;

        var doc = readDocumentFromString(String.format(
            """
                <ddm:subject xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    schemeURI="%s"
                    subjectScheme="%s"
                    valueURI="%s"
                    >
                    ABR COMPLEX
                </ddm:subject>""",
            SCHEME_URI_ABR_COMPLEX, SCHEME_ABR_COMPLEX, valueUri
        ));

        assertEquals(abrBaseUrl + "/" + termUuid,
            SubjectAbr.toAbrComplex(doc.getDocumentElement(), Map.of()));
    }
}