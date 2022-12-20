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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class LanguageTest extends BaseTest {

    private Map<String, String> getIso1() throws IOException {
        return loadCsvToMap(
            Path.of(Objects.requireNonNull(getClass().getResource("/debug-etc/iso639-1-to-dv.csv")).getPath()),
            "ISO639-1",
            "Dataverse-language"
        );
    }

    private Map<String, String> getIso2() throws IOException {
        return loadCsvToMap(
            Path.of(Objects.requireNonNull(getClass().getResource("/debug-etc/iso639-2-to-dv.csv")).getPath()),
            "ISO639-2",
            "Dataverse-language"
        );
    }

    @Test
    void toCitationBlockLanguage_should_return_English_as_the_language_name_in() throws Exception {
        var doc = readDocumentFromString("<dc:language \n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
            + "    xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "    xsi:type=\"ISO639-2\" \n"
            + "    code=\"eng\"\n"
            + ">Not used</dc:language>");
        var iso1 = getIso1();
        var iso2 = getIso2();

        assertEquals("English", Language.toCitationBlockLanguage(doc.getDocumentElement(), iso1, iso2));
    }

    @Test
    void toCitationBlockLanguage_should_return_Dutch_as_the_language_name() throws Exception {
        var doc = readDocumentFromString("<dc:language \n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
            + "    xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "    xsi:type=\"ISO639-2\" \n"
            + "    code=\"nld\"\n"
            + ">Not used</dc:language>");

        var iso1 = getIso1();
        var iso2 = getIso2();

        assertEquals("Dutch", Language.toCitationBlockLanguage(doc.getDocumentElement(), iso1, iso2));
    }

    @Test
    void toCitationBlockLanguage_should_return_French_as_the_language_name() throws Exception {
        var doc = readDocumentFromString("<dc:language \n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
            + "    xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "    xsi:type=\"ISO639-2\" \n"
            + "    code=\"fre\"\n"
            + ">Not used</dc:language>");

        var iso1 = getIso1();
        var iso2 = getIso2();

        assertEquals("French", Language.toCitationBlockLanguage(doc.getDocumentElement(), iso1, iso2));
    }

    @Test
    void toCitationBlockLanguage_should_return_German_as_the_language_name() throws Exception {
        var doc = readDocumentFromString("<dc:language \n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
            + "    xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "    xsi:type=\"ISO639-2\" \n"
            + "    code=\"deu\"\n"
            + ">Not used</dc:language>");

        var iso1 = getIso1();
        var iso2 = getIso2();

        assertEquals("German", Language.toCitationBlockLanguage(doc.getDocumentElement(), iso1, iso2));
    }

    @Test
    void toCitationBlockLanguage_should_return_None_when_type_attribute_is_not_prefixed() throws Exception {
        var doc = readDocumentFromString("<dc:language \n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
            + "    xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "    type=\"ISO639-2\" \n"
            + "    code=\"eng\"\n"
            + ">Not used</dc:language>");

        var iso1 = getIso1();
        var iso2 = getIso2();

        assertNull(Language.toCitationBlockLanguage(doc.getDocumentElement(), iso1, iso2));
    }

    @Test
    void toCitationBlockLanguage_should_return_None_when_prefix_in_type_attribute_is_not_the_correct_one() throws Exception {
        var doc = readDocumentFromString("<dc:language \n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
            + "    xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "    xsi:type=\"ISO639-2\" \n"
            + "    code=\"san\"\n"
            + ">Not used</dc:language>");

        var iso1 = getIso1();
        var iso2 = getIso2();

        assertEquals("Sanskrit (Saṁskṛta)", Language.toCitationBlockLanguage(doc.getDocumentElement(), iso1, iso2));
    }

    @Test
    void toCitationBlockLanguage_should_map_languages_with_diacritical_marks_correctly() throws Exception {
        var doc = readDocumentFromString("<dc:language \n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
            + "    xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "    xsi:type=\"ISO639-2\" \n"
            + "    code=\"eng\"\n"
            + ">Not used</dc:language>");
    }

    @Test
    void toCitationBlockLanguage_should_also_accept_the_encodingScheme_attribute_to_indicate_ISO639_2() throws Exception {
        var doc = readDocumentFromString("<dc:language \n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
            + "    xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "    encodingScheme=\"ISO639-2\" \n"
            + "    code=\"deu\"\n"
            + ">Not used</dc:language>");

        var iso1 = getIso1();
        var iso2 = getIso2();

        assertEquals("German", Language.toCitationBlockLanguage(doc.getDocumentElement(), iso1, iso2));
    }

    @Test
    void toCitationBlockLanguage_should_also_understand_two_letter_codes() throws Exception {
        var doc = readDocumentFromString("<dc:language \n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
            + "    xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "    xsi:type=\"ISO639-1\" \n"
            + "    code=\"bg\"\n"
            + ">Not used</dc:language>");

        var iso1 = getIso1();
        var iso2 = getIso2();

        assertEquals("Bulgarian", Language.toCitationBlockLanguage(doc.getDocumentElement(), iso1, iso2));
    }
}
