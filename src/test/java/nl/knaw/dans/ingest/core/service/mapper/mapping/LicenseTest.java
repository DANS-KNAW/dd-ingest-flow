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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

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
    void getLicense_should_return_URI_with_license_value_for_license_element() throws Exception {
        var xml = readDocumentFromString(
            "<dct:license xmlns:dct=\"http://purl.org/dc/terms/\" \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
                + "    xsi:type=\"dcterms:URI\"\n"
                + ">\n"
                + "    http://creativecommons.org/licenses/by-sa/4.0/\n"
                + "</dct:license>");

        var licenses = getSupportedLicenses();
        var uri = License.getLicenseUri(licenses, xml.getDocumentElement());

        assertEquals(new URI("http://creativecommons.org/licenses/by-sa/4.0/"), uri);
    }

    @Test
    void getLicense_should_return_throw_IllegalArgumentException_if_license_does_not_match_exactly() throws Exception {
        var xml = readDocumentFromString(
            "<dct:license xmlns:dct=\"http://purl.org/dc/terms/\" \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
                + "    xsi:type=\"dcterms:URI\"\n"
                + ">\n"
                + "    http://creativecommons.org/licenses/by-sa/4.0\n"
                + "</dct:license>");

        var licenses = getSupportedLicenses();

        // note that the trailing slash has been removed, a feature that was allowed before but not anymore
        assertThrows(IllegalArgumentException.class, () -> License.getLicenseUri(licenses, xml.getDocumentElement()));
    }

    @Test
    void getLicense_should_throw_an_IllegalArgumentException_if_isLicense_returns_false() throws Exception {
        // this is not a dct:license element, so it will throw
        var xml = readDocumentFromString(
            "<dct:rights \n"
                + "    xmlns:dct=\"http://purl.org/dc/terms/\" \n"
                + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n"
                + "    xsi:type=\"dcterms:URI\"\n"
                + ">\n"
                + "    http://creativecommons.org/licenses/by-sa/4.0/\n"
                + "</dct:rights>");

        var licenses = getSupportedLicenses();

        assertThrows(IllegalArgumentException.class,
            () -> License.getLicenseUri(licenses, xml.getDocumentElement()));

    }

    private List<URI> getSupportedLicenses() throws URISyntaxException {
        return List.of(
            new URI("http://creativecommons.org/licenses/by-sa/4.0/")
        );
    }

}
