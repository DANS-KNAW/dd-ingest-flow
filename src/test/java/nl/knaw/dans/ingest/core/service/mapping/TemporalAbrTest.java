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

import org.junit.jupiter.api.Test;

import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_PERIOD;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_ABR_PLUS;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_PERIOD;
import static nl.knaw.dans.ingest.core.service.DepositDatasetFieldNames.SCHEME_URI_ABR_PLUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TemporalAbrTest extends BaseTest {

    @Test
    void isAbrPeriod_should_return_true_if_schemeURI_subjectScheme_for_ABR_Periods_is_used() throws Exception {
        var doc = readDocumentFromString(String.format(
            "    <ddm:temporal\n"
                + "      xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "      schemeURI=\"%s\"\n"
                + "      subjectScheme=\"%s\"\n"
                + "      valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/c6858173-5ca2-4319-b242-f828ec53d52d\" xml:lang=\"nl\">Nieuwe Tijd</ddm:temporal>\n"
            , SCHEME_URI_ABR_PERIOD, SCHEME_ABR_PERIOD));

        assertTrue(TemporalAbr.isAbrPeriod(doc.getDocumentElement()));
    }

    @Test
    void isAbrPeriod_should_return_false_if_schemeURI_does_not_match() throws Exception {

        var doc = readDocumentFromString(String.format(
            "    <ddm:temporal\n"
                + "      xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "      schemeURI=\"%s\"\n"
                + "      subjectScheme=\"%s\"\n"
                + "      valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/c6858173-5ca2-4319-b242-f828ec53d52d\" xml:lang=\"nl\">Nieuwe Tijd</ddm:temporal>\n"
            , "https://data.cultureelerfgoed.nl/term/id/rn/NO-MATCH", SCHEME_ABR_PERIOD));

        assertFalse(TemporalAbr.isAbrPeriod(doc.getDocumentElement()));
    }

    @Test
    void isAbrPeriod_should_return_false_if_subjectScheme_does_not_match() throws Exception {

        var doc = readDocumentFromString(String.format(
            "    <ddm:temporal\n"
                + "      xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "      schemeURI=\"%s\"\n"
                + "      subjectScheme=\"%s\"\n"
                + "      valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/c6858173-5ca2-4319-b242-f828ec53d52d\" xml:lang=\"nl\">Nieuwe Tijd</ddm:temporal>\n"
            , SCHEME_URI_ABR_PERIOD, "NO MATCH"));

        assertFalse(TemporalAbr.isAbrPeriod(doc.getDocumentElement()));
    }

    @Test
    void isAbrPeriod_should_return_true_if_schemeURI_subjectScheme_for_ABR_is_used() throws Exception {

        var doc = readDocumentFromString(String.format(
            "    <ddm:temporal\n"
                + "      xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "      schemeURI=\"%s\"\n"
                + "      subjectScheme=\"%s\"\n"
                + "      valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/c6858173-5ca2-4319-b242-f828ec53d52d\" xml:lang=\"nl\">Nieuwe Tijd</ddm:temporal>\n"
            , SCHEME_URI_ABR_PLUS, SCHEME_ABR_PLUS));

        assertTrue(TemporalAbr.isAbrPeriod(doc.getDocumentElement()));
    }

    @Test
    void isAbrPeriod_should_return_false_if_schemeURI_of_ABR_does_not_match() throws Exception {

        var doc = readDocumentFromString(String.format(
            "    <ddm:temporal\n"
                + "      xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "      schemeURI=\"%s\"\n"
                + "      subjectScheme=\"%s\"\n"
                + "      valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/c6858173-5ca2-4319-b242-f828ec53d52d\" xml:lang=\"nl\">Nieuwe Tijd</ddm:temporal>\n"
            , "https://data.cultureelerfgoed.nl/term/id/rn/NO-MATCH", SCHEME_ABR_PLUS));

        assertFalse(TemporalAbr.isAbrPeriod(doc.getDocumentElement()));
    }

    @Test
    void isAbrPeriod_should_return_false_if_subjectScheme_of_ABR_does_not_match() throws Exception {

        var doc = readDocumentFromString(String.format(
            "    <ddm:temporal\n"
                + "      xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "      schemeURI=\"%s\"\n"
                + "      subjectScheme=\"%s\"\n"
                + "      valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/c6858173-5ca2-4319-b242-f828ec53d52d\" xml:lang=\"nl\">Nieuwe Tijd</ddm:temporal>\n"
            , SCHEME_URI_ABR_PLUS, "NO MATCH"));

        assertFalse(TemporalAbr.isAbrPeriod(doc.getDocumentElement()));
    }

    @Test
    void toAbrPeriod_should_create_correct_value_object() throws Exception {
        var doc = readDocumentFromString(String.format(
            "    <ddm:temporal\n"
                + "      xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "      schemeURI=\"%s\"\n"
                + "      subjectScheme=\"%s\"\n"
                + "      valueURI=\"https://data.cultureelerfgoed.nl/term/id/abr/c6858173-5ca2-4319-b242-f828ec53d52d\" xml:lang=\"nl\">Nieuwe Tijd</ddm:temporal>\n"
            , SCHEME_URI_ABR_PERIOD, SCHEME_ABR_PERIOD));

        var value = TemporalAbr.toAbrPeriod(doc.getDocumentElement());
        assertEquals("https://data.cultureelerfgoed.nl/term/id/abr/c6858173-5ca2-4319-b242-f828ec53d52d", value);
    }

}