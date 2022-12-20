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

import nl.knaw.dans.ingest.core.service.XPathEvaluator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PersonalStatementTest extends BaseTest {

    @Test
    void test_if_false() throws Exception {
        var str = "<personalDataStatement xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/agreements/\">"
            + "        <signerId easy-account=\"user001\" email=\"info@dans.knaw.nl\">MisterX</signerId>\n"
            + "        <dateSigned>2018-03-22T21:43:01.000+01:00</dateSigned>\n"
            + "        <containsPrivacySensitiveData>false</containsPrivacySensitiveData>\n"
            + "    </personalDataStatement>";

        var node = xmlReader.readXmlString(str);
        assertEquals("No", PersonalStatement.toHasPersonalDataValue(node.getDocumentElement()));
    }

    @Test
    void test_if_true() throws Exception {
        var str = "<personalDataStatement xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/agreements/\">"
            + "        <signerId easy-account=\"user001\" email=\"info@dans.knaw.nl\">MisterX</signerId>\n"
            + "        <dateSigned>2018-03-22T21:43:01.000+01:00</dateSigned>\n"
            + "        <containsPrivacySensitiveData>true</containsPrivacySensitiveData>\n"
            + "    </personalDataStatement>";

        var node = xmlReader.readXmlString(str);
        assertEquals("Yes", PersonalStatement.toHasPersonalDataValue(node.getDocumentElement()));
    }

    @Test
    void test_if_not_available() throws Exception {
//        var str = "<personalDataStatement><notAvailable/></personalDataStatement>";
        var str = "<personalDataStatement xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/agreements/\"><notAvailable/></personalDataStatement>";
        var node = xmlReader.readXmlString(str);
        assertEquals("Unknown", PersonalStatement.toHasPersonalDataValue(node.getDocumentElement()));
    }

    @Test
    void test_if_no_proper_elements_exist() throws Exception {
        var str = "<personalDataStatement xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/agreements/\"><blah/></personalDataStatement>";
        var node = xmlReader.readXmlString(str);
        assertNull(PersonalStatement.toHasPersonalDataValue(node.getDocumentElement()));
    }


    @Test
    void test_with_agreements_xml() throws Exception {
        var doc = xmlReader.readXmlString("<agreements xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/bag/metadata/agreements/ https://easy.dans.knaw.nl/schemas/bag/metadata/agreements/agreements.xsd\"\n"
            + "            xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "            xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "            xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/agreements/\"\n"
            + ">\n"
            + "    <depositAgreement>\n"
            + "        <signerId easy-account=\"user001\" email=\"info@dans.knaw.nl\">MisterX</signerId>\n"
            + "        <dcterms:dateAccepted>2018-03-22T21:43:01.000+01:00</dcterms:dateAccepted>\n"
            + "        <depositAgreementAccepted>true</depositAgreementAccepted>\n"
            + "    </depositAgreement>\n"
            + "    <personalDataStatement>\n"
            + "        <signerId easy-account=\"user001\" email=\"info@dans.knaw.nl\">MisterX</signerId>\n"
            + "        <dateSigned>2018-03-22T21:43:01.000+01:00</dateSigned>\n"
            + "        <containsPrivacySensitiveData>false</containsPrivacySensitiveData>\n"
            + "    </personalDataStatement>\n"
            + "</agreements>\n");

        var node = XPathEvaluator.nodes(doc.getDocumentElement(), "//agreements:personalDataStatement");
        var result = PersonalStatement.toHasPersonalDataValue(node.findFirst().get());

        assertEquals("No", result);
    }
}
