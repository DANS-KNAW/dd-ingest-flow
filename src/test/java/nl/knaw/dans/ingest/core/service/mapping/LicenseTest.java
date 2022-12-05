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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LicenseTest extends BaseTest {

    @Test
    void isLicense_should_return_true_if_license_element_is_found_and_has_proper_attribute() throws Exception {
        var xml = readDocumentFromString(
            "<dct:license xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"dcterms:URI\">http://creativecommons.org/licenses/by-sa/4.0/</dct:license>");
        assertTrue(License.isLicenseUri(xml.getDocumentElement()));
    }

    @Test
    void isLicense_should_return_false_if_attribute_is_not_present() throws Exception {
        var xml = readDocumentFromString("<dct:license xmlns:dct=\"http://purl.org/dc/terms/\">http://creativecommons.org/licenses/by-sa/4.0/</dct:license>");
        assertFalse(License.isLicenseUri(xml.getDocumentElement()));
    }

    @Test
    void isLicense_should_return_false_if_attribute_has_nonURI_value() throws Exception {
        var xml = readDocumentFromString(
            "<dct:license xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"dcterms:URI\">not a uri</dct:license>");
        assertFalse(License.isLicenseUri(xml.getDocumentElement()));
    }

    @Test
    void isLicense_should_return_false_if_element_is_not_dctermslicense() throws Exception {
        var xml = readDocumentFromString(
            "<dct:rights xmlns:dct=\"http://purl.org/dc/terms/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"dcterms:URI\">http://creativecommons.org/licenses/by-sa/4.0/</dct:rights>");
        assertFalse(License.isLicenseUri(xml.getDocumentElement()));
    }

    @Test
    void getLicense_should_return_URI_with_license_value_for_license_element_without_trailing_slash() throws Exception {
        var xml = readDocumentFromString(
            "<dct:license xmlns:dct=\"http://purl.org/dc/terms/\" \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
                + "    xsi:type=\"dcterms:URI\"\n"
                + ">\n"
                + "    http://creativecommons.org/licenses/by-sa/4.0/\n"
                + "</dct:license>");

        var variants = getVariantToLicense();
        var licenses = getSupportedLicenses();
        var uri = License.getLicenseUri(licenses, variants, xml.getDocumentElement());

        assertEquals(new URI("http://creativecommons.org/licenses/by-sa/4.0"), uri);
    }

    @Test
    void getLicense_should_throw_an_IllegalArgumentException_if_isLicense_returns_false() throws Exception {
        var xml = readDocumentFromString(
            "<dct:rights \n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\" \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
                + "    xsi:type=\"dcterms:URI\"\n"
                + ">\n"
                + "    http://creativecommons.org/licenses/by-sa/4.0/\n"
                + "</dct:rights>");

        var variants = getVariantToLicense();
        var licenses = getSupportedLicenses();

        assertThrows(IllegalArgumentException.class,
            () -> License.getLicenseUri(licenses, variants, xml.getDocumentElement()));

    }

    @Test
    void getLicense_should_Return_a_supported_license_given_a_configured_variant() throws Exception {
        var doc = readDocumentFromString(
            "<dct:license \n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\" \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
                + "    xsi:type=\"dcterms:URI\"\n"
                + ">\n"
                + "    http://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html\n"
                + "</dct:license>");

        var variants = getVariantToLicense();
        var licenses = getSupportedLicenses();

        var uri = License.getLicenseUri(licenses, variants, doc.getDocumentElement());

        assertEquals(new URI("http://www.gnu.org/licenses/old-licenses/gpl-2.0"), uri);
    }

    @Test
    void getLicense_should_Accept_supported_license_with_either_http_or_https_scheme() throws Exception {
        var doc = readDocumentFromString(
            "<dct:license \n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\" \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
                + "    xsi:type=\"dcterms:URI\"\n"
                + ">\n"
                + "    https://www.gnu.org/licenses/old-licenses/gpl-2.0\n"
                + "</dct:license>");

        var variants = getVariantToLicense();
        var licenses = getSupportedLicenses();

        var uri = License.getLicenseUri(licenses, variants, doc.getDocumentElement());
        assertEquals(new URI("http://www.gnu.org/licenses/old-licenses/gpl-2.0"), uri);
    }

    @Test
    void getLicense_should_Accept_empty_license_variants_file() throws Exception {
        var doc = readDocumentFromString(
            "<dct:license \n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\" \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
                + "    xsi:type=\"dcterms:URI\"\n"
                + ">\n"
                + "    http://www.gnu.org/licenses/old-licenses/gpl-2.0\n"
                + "</dct:license>");

        var variants = getVariantToLicense();
        var licenses = getSupportedLicenses();

        var uri = License.getLicenseUri(licenses, variants, doc.getDocumentElement());
        assertEquals(new URI("http://www.gnu.org/licenses/old-licenses/gpl-2.0"), uri);
    }

    private Map<String, String> getVariantToLicense() throws IOException {
        return getMap(Path.of(Objects.requireNonNull(
            getClass().getResource("/debug-etc/license-uri-variants.csv")).getPath())
        );
    }

    private List<URI> getSupportedLicenses() throws IOException, URISyntaxException {
        return getUriList(Path.of(Objects.requireNonNull(
            getClass().getResource("/debug-etc/supported-licenses.txt")
        ).getPath()));
    }

}
