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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbrReportTypeTest extends BaseTest {

    @Test
    void isAbrReportType_should_return_true_when_properties_match() throws Exception {
        var doc = readDocumentFromString("<ddm:reportNumber \n"
            + "            xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "            subjectScheme=\"ABR Rapporten\" \n"
            + "            schemeURI=\"https://data.cultureelerfgoed.nl/term/id/abr/7a99aaba-c1e7-49a4-9dd8-d295dbcc870e\">value</ddm:reportNumber>");

        assertTrue(AbrReportType.isAbrReportType(doc.getDocumentElement()));
    }

    @Test
    void isAbrReportType_should_return_false_when_subjectScheme_does_not_match() throws Exception {
        var doc = readDocumentFromString("<ddm:reportNumber \n"
            + "            xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "            subjectScheme=\"INVALID\" \n"
            + "            schemeURI=\"https://data.cultureelerfgoed.nl/term/id/abr/7a99aaba-c1e7-49a4-9dd8-d295dbcc870e\">value</ddm:reportNumber>");

        assertFalse(AbrReportType.isAbrReportType(doc.getDocumentElement()));
    }

    @Test
    void isAbrReportType_should_return_false_when_schemeURI_does_not_match() throws Exception {
        var doc = readDocumentFromString("<ddm:reportNumber \n"
            + "            xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "            subjectScheme=\"ABR Rapporten\" \n"
            + "            schemeURI=\"https://fake.data.cultureelerfgoed.nl/term/id/abr/7a99aaba-c1e7-49a4-9dd8-d295dbcc870e\">value</ddm:reportNumber>");

        assertFalse(AbrReportType.isAbrReportType(doc.getDocumentElement()));
    }

    @Test
    void toAbrRapportType_should_return_valueURI() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:reportNumber \n"
                + "            xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "            subjectScheme=\"ABR Rapporten\" \n"
                + "            valueURI=\"https://dar.dans.knaw.nl/\" \n"
                + "            schemeURI=\"https://data.cultureelerfgoed.nl/term/id/abr/7a99aaba-c1e7-49a4-9dd8-d295dbcc870e\" />");

        assertEquals("https://dar.dans.knaw.nl/", AbrReportType.toAbrRapportType(doc.getDocumentElement()));
    }

    @Test
    void toAbrRapportType_should_return_null_when_valueURI_is_missing() throws Exception {
        var doc = readDocumentFromString(
            "<ddm:reportNumber \n"
                + "            xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
                + "            subjectScheme=\"ABR Rapporten\" \n"
                + "            schemeURI=\"https://data.cultureelerfgoed.nl/term/id/abr/7a99aaba-c1e7-49a4-9dd8-d295dbcc870e\" />");

        assertNull(AbrReportType.toAbrRapportType(doc.getDocumentElement()));
    }
}